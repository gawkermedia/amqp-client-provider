package com.kinja.amqp

import java.util.concurrent.TimeoutException

import akka.actor.{ Actor, ActorSystem, Cancellable, Props }
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
	producers: Map[String, AmqpProducerInterface],
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

	private case class StartSchedule(ec: ExecutionContext)
	private case object StopSchedule
	private case class StopLocking(ec: ExecutionContext)
	private case class ResumeLocking(ec: ExecutionContext)

	private val resendSchedule = actorSystem.actorOf(Props(new Actor {

		def createSchedule(lock: Boolean)(implicit ec: ExecutionContext): Cancellable =
			actorSystem.scheduler.schedule(initialDelay, bufferProcessInterval)(
				processMessageBuffer(lock)
			)

		def receive = {
			case StartSchedule(ec) =>
				context.become(repeating(createSchedule(true)(ec)))
		}

		def repeating(resendSchedule: Cancellable): Receive = {
			case StopSchedule =>
				resendSchedule.cancel()
				context.unbecome()
			case StopLocking(ec) =>
				resendSchedule.cancel()
				context.become(repeating(createSchedule(false)(ec)))
			case ResumeLocking(ec) =>
				resendSchedule.cancel()
				context.become(repeating(createSchedule(true)(ec)))
		}

	}))

	/**
	 * Schedules message resend logic periodically
	 * @param ec Execution context used for scheduling and resend logic
	 */
	def startSchedule(implicit ec: ExecutionContext): Unit = resendSchedule ! StartSchedule(ec)

	def stopLocking(implicit ec: ExecutionContext): Unit = resendSchedule ! StopLocking(ec)
	def resumeLocking(implicit ec: ExecutionContext): Unit = resendSchedule ! ResumeLocking(ec)

	private def processMessageBuffer(lock: Boolean)(implicit ec: ExecutionContext): Unit = {
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
		if (lock) {
			tryWithLogging(
				messageStore.lockRowsOlderThan(minMsgAge.toSeconds, messageLockTimeOutAfter.toSeconds, batchSize),
				"Locked %d rows"
			)
			tryWithLogging(
				resendLocked(),
				"Resent %d messages"
			)
		}
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
				val messagesToProducer = messages.filter(_.exchangeName == exchange)

				resendAndDelete(messagesToProducer, producer, republishTimeout)
			case _ => ()
		}
		messages.length
	}

	/**
	 * Resend the messages in the list and if managed to publish, deletes the message
	 */
	private def resendAndDelete(
		msgs: List[Message],
		producer: AmqpProducerInterface,
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

	def shutdown(): Unit = {
		resendSchedule ! StopSchedule
	}
}
