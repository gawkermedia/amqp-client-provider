package com.kinja.amqp

import java.util.concurrent.TimeoutException

import akka.actor.ActorSystem
import com.kinja.amqp.model.Message
import com.kinja.amqp.persistence.MessageStore
import org.slf4j.{ Logger => Slf4jLogger }

import scala.concurrent.duration._
import scala.concurrent.{ Await, ExecutionContext }
import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }

/**
 * @param initialDelay The delay to start scheduling after
 * @param bufferProcessInterval Interval between two scheduled actions
 * @param minMsgAge The minimum age of the message to resend
 * @param maxMultiConfAge The max age of the confirmations for multiple msgs before deletion
 * @param maxSingleConfAge The max age of the confirmations for a single message before deletion
 * @param republishTimeout The timeout which we can wait when republishing the msg
 * @param batchSize The max number of messages that are processed in each iteration
 */
class MessageBufferProcessor(
	actorSystem: ActorSystem,
	messageStore: MessageStore,
	producers: Map[String, AmqpProducer],
	logger: Slf4jLogger
)(
	initialDelay: FiniteDuration,
	bufferProcessInterval: FiniteDuration,
	minMsgAge: FiniteDuration,
	maxMultiConfAge: FiniteDuration,
	maxSingleConfAge: FiniteDuration,
	republishTimeout: FiniteDuration,
	batchSize: Int,
	messageLockTimeOutAfter: FiniteDuration
) {

	/**
	 * Schedules message resend logic periodically
	 * @param ec Execution context used for scheduling and resend logic
	 */
	def startSchedule(implicit ec: ExecutionContext): Unit = ignore {
		actorSystem.scheduler.schedule(initialDelay, bufferProcessInterval)(
			processMessageBuffer()
		)
	}

	private def processMessageBuffer()(implicit ec: ExecutionContext): Unit = {
		logger.debug("Processing message buffer...")
		tryWithLogging(
			messageStore.deleteMatchingMessagesAndSingleConfirms(),
			"Deleted %d matching messages and single confirms"
		)
		tryWithLogging(
			messageStore.deleteMessagesWithMatchingMultiConfirms(),
			"Deleted %d messages which had matching multi confirms"
		)
		tryWithLogging(
			messageStore.deleteMultiConfIfNoMatchingMsg(maxMultiConfAge.toSeconds),
			"Deleted %d multiple confirms which had no matching messages"
		)
		tryWithLogging(
			messageStore.deleteOldSingleConfirms(maxSingleConfAge.toSeconds),
			"Deleted %d old single confirmations"
		)
		tryWithLogging(
			messageStore.lockRowsOlderThan(minMsgAge.toSeconds, messageLockTimeOutAfter.toSeconds, batchSize),
			"Locked %d rows"
		)
		tryWithLogging(
			resendLocked(),
			"Resent %d messages"
		)
	}

	private def tryWithLogging(f: => Int, debugLog: String): Unit = {
		try {
			val d = f
			logger.debug(debugLog.format(d))
		} catch {
			case NonFatal(t) => logger.error(s"[RabbitMQ] Exception while processing RabbitMQ message buffer: $t")
		}
	}

	private def resendLocked()(implicit ec: ExecutionContext): Int = {
		// in case we have more locked rows than the batch size (failed to process after previous lock)
		val batchSizeWithExtraGap = batchSize * 2
		val messages = messageStore.loadLockedMessages(batchSizeWithExtraGap)
		scala.util.Random.shuffle(producers).foreach {
			case (exchange, producer) =>
				val messagesToProducer = messages.filter(_.exchangeName == producer.exchange.name)

				resendAndDelete(messagesToProducer, producer, republishTimeout)
		}
		messages.length
	}

	/**
	 * Resend the messages in the list and if managed to publish, deletes the message
	 */
	private def resendAndDelete(
		msgs: List[Message],
		producer: AmqpProducer,
		republishTimeout: FiniteDuration
	)(implicit ec: ExecutionContext): Unit = {
		msgs.foreach { msg =>
			val result = Try(Await.result(producer.publish(msg.routingKey, msg.message), republishTimeout))
			result map { _ =>
				messageStore.deleteMessage(
					msg.id.getOrElse(throw new IllegalStateException("Got a message without an id from database"))
				)
			} recover {
				case ex: TimeoutException =>
					// in this case message will resaved in the publish loop, so we can delete it here
					messageStore.deleteMessage(
						msg.id.getOrElse(throw new IllegalStateException("Got a message without an id from database"))
					)
					logger.warn(s"""[RabbitMQ] Couldn't resend message: $msg, ${ex.getMessage}""")
				case ex =>
					logger.warn(s"""[RabbitMQ] Couldn't resend message: $msg, ${ex.getMessage}""")
			}
		}
	}
}
