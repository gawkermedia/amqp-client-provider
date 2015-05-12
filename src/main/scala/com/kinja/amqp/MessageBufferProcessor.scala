package com.kinja.amqp

import java.util.concurrent.TimeoutException

import akka.actor.ActorSystem
import com.kinja.amqp.model.Message
import com.kinja.amqp.persistence.MessageStore
import org.slf4j.{Logger => Slf4jLogger}
import play.api.libs.json.Json

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/**
 * @param initialDelay The delay to start scheduling after
 * @param interval Interval between two scheduled actions
 * @param minMsgAge The minimum age of the message to resend
 * @param maxMultiConfAge The max age of the confirmations for multiple msgs before deletion
 * @param maxSingleConfAge The max age of the confirmations for a single message before deletion
 * @param republishTimeout The timeout which we can wait when republishing the msg
 * @param limit The max number of messages that are processed in each iteration
 */
class MessageBufferProcessor(
	actorSystem: ActorSystem,
	messageStore: MessageStore,
	producers: Map[String, AmqpProducer],
	logger: Slf4jLogger
)(
	initialDelay: FiniteDuration,
	interval: FiniteDuration,
	minMsgAge: FiniteDuration,
	maxMultiConfAge: FiniteDuration,
	maxSingleConfAge: FiniteDuration,
	republishTimeout: FiniteDuration,
	limit: Int,
	messageLockTimeOutAfter: FiniteDuration
) {

	/**
	 * Schedules message resend logic periodically
	 * @param ec Execution context used for scheduling and resend logic
	 */
	def startSchedule(implicit ec: ExecutionContext): Unit = {
		actorSystem.scheduler.schedule(initialDelay, interval)(
			processMessageBuffer()
		)
	}

	private def processMessageBuffer()(implicit ec: ExecutionContext): Unit = {
		tryWithLogging(messageStore.deleteMatchingMessagesAndSingleConfirms())
		tryWithLogging(messageStore.deleteMessagesWithMatchingMultiConfirms())
		tryWithLogging(messageStore.deleteMultiConfIfNoMatchingMsg(maxMultiConfAge.toSeconds))
		tryWithLogging(messageStore.deleteSingleConfIfNoMatchingMsg(maxSingleConfAge.toSeconds))
		tryWithLogging(messageStore.lockRowsOlderThan(minMsgAge.toSeconds, messageLockTimeOutAfter.toSeconds, limit))
		tryWithLogging(resendLocked())
	}

	private def tryWithLogging(f: => Unit)(implicit ec: ExecutionContext): Unit = {
		try {
			f
		} catch {
			case NonFatal(t) => logger.error(s"Exception while processing RabbitMQ message buffer: $t")
		}
	}

	private def resendLocked()(implicit ec: ExecutionContext): Unit = {
		val messages = messageStore.loadLockedMessages(limit * 2)
		scala.util.Random.shuffle(producers).foreach {
			case (exchange, producer) =>
				val messagesToProducer = messages.filter(_.exchangeName == producer.exchange.name)

				resendAndDelete(messagesToProducer, producer, republishTimeout)
		}
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
			val result = Try(Await.result(producer.publish(msg.routingKey, Json.parse(msg.message)), republishTimeout))
			result match {
				case Success(_) =>
					messageStore.deleteMessage(
						msg.id.getOrElse(throw new IllegalStateException("Got a message without an id from database"))
					)
				case Failure(ex: TimeoutException) =>
					// in this case message will resaved in the publish loop, so we can delete it here
					messageStore.deleteMessage(
						msg.id.getOrElse(throw new IllegalStateException("Got a message without an id from database"))
					)
					logger.warn(s"""Couldn't resend message: $msg, ${ex.getMessage}""")
				case Failure(ex) =>
					logger.warn(s"""Couldn't resend message: $msg, ${ex.getMessage}""")
			}
		}
	}
}
