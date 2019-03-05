package com.kinja.amqp

import java.util.UUID
import java.util.concurrent.TimeoutException

import akka.actor.{ Actor, ActorSystem, Cancellable, Props, Stash }
import com.kinja.amqp.model.{ FailedMessage, Message, MessageLike }
import com.kinja.amqp.persistence.MessageStore
import com.kinja.amqp.utils.Utils
import org.slf4j.{ Logger => Slf4jLogger }

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success }
import scala.util.control.NonFatal

/**
 * @param initialDelay The delay to start scheduling after
 * @param bufferProcessInterval Interval between two scheduled actions
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
	republishTimeout: FiniteDuration,
	batchSize: Int
) {

	private case class StartSchedule(ec: ExecutionContext)
	private case object StopSchedule
	private case class StopLocking(ec: ExecutionContext)
	private case class ResumeLocking(ec: ExecutionContext)
	private case object RunScheduled
	private case object ProcessingFinished

	private val resendSchedule = actorSystem.actorOf(Props(new Actor with Stash {

		def createSchedule(implicit ec: ExecutionContext): Cancellable =
			actorSystem.scheduler.schedule(initialDelay, bufferProcessInterval, self, RunScheduled)

		def receive: Receive = idle

		def idle: Receive = {
			case StartSchedule(ec) =>
				context.become(repeating(0, lock = true, ec, createSchedule(ec)))
		}

		@SuppressWarnings(Array("org.wartremover.warts.Recursion"))
		def repeating(iteration: Int, lock: Boolean, ec: ExecutionContext, resendSchedule: Cancellable): Receive = {
			case StopSchedule =>
				ignore(resendSchedule.cancel())
				context.become(idle)
			case StopLocking(ec) =>
				ignore(resendSchedule.cancel())
				context.become(repeating(iteration, lock = false, ec, createSchedule(ec)))
			case ResumeLocking(ec) =>
				ignore(resendSchedule.cancel())
				context.become(repeating(iteration, lock = true, ec, createSchedule(ec)))
			case RunScheduled =>
				implicit val stepId: UUID = UUID.randomUUID()
				val fullProcess = iteration > 99
				if (fullProcess)
					context.become(processing(iteration = 0, lock, ec, resendSchedule))
				else
					context.become(processing(iteration + 1, lock, ec, resendSchedule))
				val me = self
				val actorSystem = context.system
				Utils.withTimeout(
					name = s"ProcessingMessageBuffer(id=$stepId)",
					step = processMessageBuffer(lock, fullProcess)(stepId, ec),
					timeout = FiniteDuration(bufferProcessInterval.length * 3, bufferProcessInterval.unit)
				)(actorSystem).onComplete {
						case Success(_) =>
							logger.debug(s"[RabbitMQ][ProcessingMessageBuffer(id=$stepId)] Succeed.")
							me ! ProcessingFinished
						case Failure(e) =>
							logger.error(s"[RabbitMQ][ProcessingMessageBuffer(id=$stepId)] Failed with.", e)
							me ! ProcessingFinished
					}(ec)
		}

		def processing(
			iteration: Int,
			lock: Boolean,
			ec: ExecutionContext,
			resendSchedule: Cancellable)(
			implicit
			stepId: UUID): Receive = {
			case RunScheduled =>
				logger.warn(s"[RabbitMQ][ProcessingMessageBuffer(id=$stepId)] Skipping scheduled event in processing state.")
			case ProcessingFinished =>
				context.become(repeating(iteration, lock, ec, resendSchedule))
				unstashAll()
			case _ => stash()
		}
	}))

	/**
	 * Schedules message resend logic periodically
	 * @param ec Execution context used for scheduling and resend logic
	 */
	def startSchedule(implicit ec: ExecutionContext): Unit = resendSchedule ! StartSchedule(ec)

	def stopLocking(implicit ec: ExecutionContext): Unit = resendSchedule ! StopLocking(ec)
	def resumeLocking(implicit ec: ExecutionContext): Unit = resendSchedule ! ResumeLocking(ec)

	private def processMessageBuffer(lock: Boolean, fullProcess: Boolean)(implicit stepId: UUID, ec: ExecutionContext): Future[Unit] = {
		logger.debug(s"[RabbitMQ][ProcessingMessageBuffer(id=$stepId)]Processing message buffer...")
		for {
			hasMessageToProcess <- if (fullProcess) Future.successful(true) else messageStore.hasMessageToProcess()
			_ <- if (hasMessageToProcess) {
				for {
					_ <- withLogging(
						messageStore.deleteMatchingMessagesAndSingleConfirms(),
						s"[RabbitMQ][ProcessingMessageBuffer(id=$stepId)] Deleted %d matching messages and single confirms"
					)
					_ <- withLogging(
						messageStore.deleteMessagesWithMatchingMultiConfirms(),
						s"[RabbitMQ][ProcessingMessageBuffer(id=$stepId)] Deleted %d messages which had matching multi confirms"
					)
					_ <- withLogging(
						messageStore.deleteMultiConfIfNoMatchingMsg(),
						s"[RabbitMQ][ProcessingMessageBuffer(id=$stepId)] Deleted %d multiple confirms which had no matching messages"
					)
					_ <- withLogging(
						messageStore.deleteOldSingleConfirms(),
						s"[RabbitMQ][ProcessingMessageBuffer(id=$stepId)] Deleted %d old single confirmations"
					)
					_ <- if (lock) {
						for {
							_ <- withLogging(
								messageStore.lockOldRows(batchSize),
								s"[RabbitMQ][ProcessingMessageBuffer(id=$stepId)] Locked %d rows"
							)
							_ <- withLogging(
								resendLocked(),
								s"[RabbitMQ][ProcessingMessageBuffer(id=$stepId)] Resent %d messages"
							)
						} yield ()
					} else {
						Future.successful(())
					}
				} yield ()
			} else {
				logger.debug(s"[RabbitMQ][ProcessingMessageBuffer(id=$stepId)] No message to process.")
				Future.successful(())
			}
		} yield ()
	}

	private def withLogging(f: => Future[Int], debugLog: String)(implicit ec: ExecutionContext, stepId: UUID): Future[Unit] = {
		f
			.map(d => logger.debug(debugLog.format(d)))
			.recover {
				case NonFatal(t) => logger.error(s"[RabbitMQ][ProcessingMessageBuffer(id=$stepId)] Exception while processing RabbitMQ message buffer", t)
			}
	}

	private def resendLocked()(implicit ec: ExecutionContext, stepId: UUID): Future[Int] = {
		// in case we have more locked rows than the batch size (failed to process after previous lock)
		val batchSizeWithExtraGap = batchSize * 2
		for {
			messages <- messageStore.loadLockedMessages(batchSizeWithExtraGap)
			_ <- Future
				.sequence(scala.util.Random.shuffle(producers)
					.map {
						case (exchange, producer) =>
							val messagesToProducer = messages.filter(_.exchangeName == exchange)
							resendAndDelete(messagesToProducer, producer, republishTimeout)
						case _ => Future.successful(())
					})
		} yield messages.length
	}

	private def deleteMessage(msg: MessageLike)(implicit ec: ExecutionContext): Future[Unit] = msg match {
		case FailedMessage(None, _, _, _, _) =>
			throw new IllegalStateException("Got a message without an id from database")
		case FailedMessage(Some(id), _, _, _, _) => messageStore.deleteFailedMessage(id)
		case Message(_, _, _, channelId, deliveryTag, _) =>
			messageStore.deleteMessage(channelId, deliveryTag).map(_ => ())
	}

	/**
	 * Resend the messages in the list and if managed to publish, deletes the message
	 */
	private def resendAndDelete(
		msgs: List[MessageLike],
		producer: AmqpProducerInterface,
		republishTimeout: FiniteDuration
	)(implicit ec: ExecutionContext, stepId: UUID): Future[Unit] = {
		val r = msgs.map { msg =>
			val resultF = Utils.withTimeout("publish", producer.publish[String](msg.routingKey, msg.message), republishTimeout)(actorSystem)
			resultF.flatMap { _ => deleteMessage(msg) } recoverWith {
				case ex: TimeoutException =>
					// in this case message will resaved in the publish loop, so we can delete it here
					deleteMessage(msg).map(_ =>
						logger.warn(s"""[RabbitMQ][ProcessingMessageBuffer(id=$stepId)] Couldn't resend message: $msg, ${ex.getMessage}""")
					)
				case ex =>
					Future.successful(logger.warn(s"""[RabbitMQ][ProcessingMessageBuffer(id=$stepId)] Couldn't resend message: $msg, ${ex.getMessage}"""))
			}
		}
		Future.sequence(r).map(_ => ())
	}

	def shutdown(): Unit = {
		resendSchedule ! StopSchedule
	}
}
