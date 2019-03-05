package com.kinja.amqp.persistence

import java.util.UUID

import akka.actor.{ ActorRef, ActorSystem, Cancellable, Props }
import akka.pattern.ask
import akka.util.Timeout
import com.kinja.amqp.ignore
import com.kinja.amqp.model.{ Message, MessageConfirmation, MessageLike }
import com.kinja.amqp.utils.Utils
import org.slf4j.{ Logger => Slf4jLogger }

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.control.NonFatal

class InMemoryMessageBufferDecorator(
	messageStore: MessageStore,
	actorSystem: ActorSystem,
	logger: Slf4jLogger,
	memoryFlushInterval: FiniteDuration,
	memoryFlushChunkSize: Int,
	memoryFlushTimeOut: FiniteDuration,
	askTimeout: FiniteDuration
)(implicit val ec: ExecutionContext) extends MessageStore {

	private implicit val timeout: Timeout = Timeout(askTimeout)

	private val inMemoryMessageBuffer: ActorRef = actorSystem.actorOf(Props(new InMemoryMessageBuffer))

	logger.debug("Scheduling memory flusher...")

	private val memoryFlushSchedule: Cancellable = actorSystem.scheduler.schedule(
		1.second, memoryFlushInterval
	)(ignore(flushMemoryBufferToMessageStore()))

	logger.debug("Memory flusher scheduled")

	override def hasMessageToProcess(): Future[Boolean] = {
		messageStore.hasMessageToProcess()
	}

	override def saveConfirmations(confirms: List[MessageConfirmation]): Future[Unit] = {
		val (multiples, singles) = confirms.partition(_.multiple)
		// we don't save every multiple confirmation here,
		// just collect (and increment) them and save all at once in the flush loop
		if (multiples.nonEmpty) {
			inMemoryMessageBuffer ! MultipleConfirmations(multiples)
		}
		if (singles.nonEmpty) {
			messageStore.saveConfirmations(singles)
		} else {
			Future.successful(())
		}
	}

	override def deleteFailedMessage(id: Long): Future[Unit] = {
		messageStore.deleteFailedMessage(id)
	}

	override def deleteOldSingleConfirms(): Future[Int] = {
		messageStore.deleteOldSingleConfirms()
	}

	override def lockOldRows(limit: Int): Future[Int] = {
		messageStore.lockOldRows(limit)
	}

	override def saveMessages(msgs: List[MessageLike]): Future[Unit] = {
		if (msgs.nonEmpty) {
			inMemoryMessageBuffer ! SaveMessages(msgs)
		}
		Future.successful(())
	}

	override def deleteMultiConfIfNoMatchingMsg(): Future[Int] = {
		messageStore.deleteMultiConfIfNoMatchingMsg()
	}

	override def deleteMatchingMessagesAndSingleConfirms(): Future[Int] = {
		messageStore.deleteMatchingMessagesAndSingleConfirms()
	}

	override def deleteMessage(channelId: String, deliveryTag: Long): Future[Boolean] = {
		val matched: Future[Any] = inMemoryMessageBuffer ? DeleteMessageUponConfirm(channelId, deliveryTag)

		matched.flatMap {
			case false => messageStore.deleteMessage(channelId, deliveryTag)
			case _ => Future.successful(true)
		}
	}

	override def loadLockedMessages(limit: Int): Future[List[MessageLike]] = {
		messageStore.loadLockedMessages(limit)
	}

	override def deleteMessagesWithMatchingMultiConfirms(): Future[Int] = {
		messageStore.deleteMessagesWithMatchingMultiConfirms()
	}

	private def flushMemoryBufferToMessageStore(): Future[Unit] = {
		logger.info("Flushing memory buffer to message store...")

		if (logger.isInfoEnabled) {
			ignore(inMemoryMessageBuffer ? LogBufferStatistics(logger))
		}

		val r = for {
			_ <- handleMessagesResponseFromBuffer(
				inMemoryMessageBuffer ? RemoveMessagesOlderThan(memoryFlushInterval.toMillis)
			)

			_ <- handleConfirmationsResponseFromBuffer(
				inMemoryMessageBuffer ? RemoveMultipleConfirmations
			)
		} yield ()
		r.recover {
			case NonFatal(t) => logger.error(
				s"[RabbitMQ] Exception while trying to flush in-memory buffer (scheduled): ${t.getMessage}", t
			)
		}
	}

	private def tryWithLogging(name: String, f: => Unit): Unit = {
		try {
			f
		} catch {
			case NonFatal(t) => logger.error(
				s"[RabbitMQ] Exception while trying to flush in-memory buffer ($name): ${t.getMessage}", t
			)
		}
	}

	@SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
	private def handleMessagesResponseFromBuffer(response: Future[Any]): Future[Unit] = {
		val messagesSent: Future[Unit] = response flatMap { messages =>
			val messageList = messages.asInstanceOf[List[Message]]
			if (messageList.nonEmpty) {
				val flushId = UUID.randomUUID()
				logger.info(s"MessageFlushing[id = $flushId] started with ${messageList.size} messages ...")
				Future.sequence(messageList
					.grouped(memoryFlushChunkSize)
					.map(group => {
						logger.info(s"MessageFlushing[id = $flushId] Flushing ${group.length} messages ...")
						messageStore.saveMessages(group)
					}))
					.map(_ => logger.info(s"MessageFlushing[id=$flushId] finished."))
			} else {
				Future.successful(())
			}
		}
		Utils.withTimeout("handleMessagesResponseFromBuffer", messagesSent, memoryFlushTimeOut)(actorSystem)
	}

	@SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
	private def handleConfirmationsResponseFromBuffer(response: Future[Any]): Future[Unit] = {
		val confirmationsSent: Future[Unit] = response flatMap { confirmations =>
			val confirmationList = confirmations.asInstanceOf[List[MessageConfirmation]]
			if (confirmationList.nonEmpty) {
				val flushId = UUID.randomUUID()
				logger.info(s"ConfirmationFlushing[id = $flushId] started with ${confirmationList.size} confirmations ...")
				Future.sequence(confirmationList
					.grouped(memoryFlushChunkSize)
					.map(group => {
						logger.info(s"ConfirmationFlushing[id = $flushId] Flushing ${group.length} confirmations...")
						messageStore.saveConfirmations(group)
					}))
					.map(_ => logger.info(s"ConfirmationFlushing[id = $flushId] finished."))
			} else {
				Future.successful(())
			}
		}
		Utils.withTimeout("handleConfirmationsResponseFromBuffer", confirmationsSent, memoryFlushTimeOut)(actorSystem)
	}

	override def shutdown(): Future[Unit] = {
		logger.info("Shutdown: flushing memory buffer to message store...")

		ignore(memoryFlushSchedule.cancel())

		if (logger.isInfoEnabled) {
			ignore(inMemoryMessageBuffer ? LogBufferStatistics(logger))
		}
		val r = for {
			_ <- handleMessagesResponseFromBuffer(
				inMemoryMessageBuffer ? GetAllMessages
			)
			_ <- handleConfirmationsResponseFromBuffer(
				inMemoryMessageBuffer ? RemoveMultipleConfirmations
			)
		} yield ()

		r.recover {
			case NonFatal(t) => logger.error(
				s"[RabbitMQ] Exception while trying to flush in-memory buffer (shutdown): ${t.getMessage}", t
			)
		}
	}
}
