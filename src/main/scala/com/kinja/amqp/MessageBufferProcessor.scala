package com.kinja.amqp

import java.util.UUID
import java.util.concurrent.TimeoutException

import akka.actor.{ Actor, ActorSystem, Cancellable, Props, Stash }
import com.kinja.amqp.model.FailedMessage
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
 */
@SuppressWarnings(Array("org.wartremover.warts.StringPlusAny"))
class MessageBufferProcessor(
	actorSystem: ActorSystem,
	messageStore: MessageStore,
	producers: Map[String, AmqpProducerInterface],
	logger: Slf4jLogger
)(
	initialDelay: FiniteDuration,
	bufferProcessInterval: FiniteDuration,
	republishTimeout: FiniteDuration
) {

	private case class StartSchedule(ec: ExecutionContext)
	private case object StopSchedule
	private case class StopLocking(ec: ExecutionContext)
	private case class ResumeLocking(ec: ExecutionContext)
	private case object RunScheduled
	private case object ProcessingFinished

	private val resendSchedule = actorSystem.actorOf(Props(new Actor with Stash {

		def createSchedule(implicit ec: ExecutionContext): Cancellable =
			actorSystem.scheduler.scheduleAtFixedRate(initialDelay, bufferProcessInterval, self, RunScheduled)

		def receive: Receive = idle

		def idle: Receive = {
			case StartSchedule(ec) =>
				context.become(repeating(lock = true, ec, createSchedule(ec)))
		}

		@SuppressWarnings(Array("org.wartremover.warts.Recursion"))
		def repeating(lock: Boolean, ec: ExecutionContext, resendSchedule: Cancellable): Receive = {
			case StopSchedule =>
				ignore(resendSchedule.cancel())
				context.become(idle)
			case StopLocking(ec) =>
				ignore(resendSchedule.cancel())
				context.become(repeating(lock = false, ec, createSchedule(ec)))
			case ResumeLocking(ec) =>
				ignore(resendSchedule.cancel())
				context.become(repeating(lock = true, ec, createSchedule(ec)))
			case RunScheduled =>
				implicit val stepId: UUID = UUID.randomUUID()
				context.become(processing(lock, ec, resendSchedule))
				val me = self
				val actorSystem = context.system
				Utils.withTimeout(
					name = s"ProcessingMessageBuffer(id=$stepId)",
					step = processMessageBuffer(lock)(stepId, ec),
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
			lock: Boolean,
			ec: ExecutionContext,
			resendSchedule: Cancellable)(
			implicit
			stepId: UUID): Receive = {
			case RunScheduled =>
				logger.warn(s"[RabbitMQ][ProcessingMessageBuffer(id=$stepId)] Skipping scheduled event in processing state.")
			case ProcessingFinished =>
				context.become(repeating(lock, ec, resendSchedule))
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

	private def processMessageBuffer(lock: Boolean)(implicit stepId: UUID, ec: ExecutionContext): Future[Unit] = {
		logger.debug(s"[RabbitMQ][ProcessingMessageBuffer(id=$stepId)]Processing message buffer...")
		(for {
			hasMessageToProcess <- messageStore.cleanup()
			_ <- if (hasMessageToProcess) {
				logger.debug(s"[RabbitMQ][ProcessingMessageBuffer(id=$stepId)]Cleanup (some messages removed)")
				if (lock) {
					for {
						messages <- messageStore.lockAndLoad()
						shuffled = scala.util.Random.shuffle(producers)
						_ <- Future
							.sequence(shuffled.map {
								case (exchange, producer) =>
									val messagesToProducer = messages.filter(_.exchangeName == exchange)
									resendAndDelete(messagesToProducer, producer, republishTimeout)
							})
						_ = logger.debug(s"[RabbitMQ][ProcessingMessageBuffer(id=$stepId)] Resent ${messages.length} messages")
					} yield ()
				} else {
					logger.debug(s"[RabbitMQ][ProcessingMessageBuffer(id=$stepId)] Locking is not active, no message resend")
					Future.successful(())
				}
			} else {
				logger.debug(s"[RabbitMQ][ProcessingMessageBuffer(id=$stepId)] No message to process.")
				Future.successful(())
			}
		} yield ()).recover {
			case NonFatal(t) => logger.error(s"[RabbitMQ][ProcessingMessageBuffer(id=$stepId)] Exception while processing RabbitMQ message buffer", t)
		}
	}

	private def deleteMessage(msg: FailedMessage): Future[Unit] = msg match {
		case FailedMessage(None, _, _, _, _, _) =>
			Future.failed[Unit](new IllegalStateException("Got a message without an id from database"))
		case FailedMessage(Some(id), _, _, _, _, _) => messageStore.deleteFailedMessage(id)
	}

	/**
	 * Resend the messages in the list and if managed to publish, deletes the message
	 */
	private def resendAndDelete(
		msgs: List[FailedMessage],
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
