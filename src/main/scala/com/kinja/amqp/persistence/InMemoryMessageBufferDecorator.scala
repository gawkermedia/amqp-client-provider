package com.kinja.amqp.persistence

import akka.actor.{ Cancellable, ActorRef, ActorSystem, Props }
import akka.pattern.ask
import akka.util.Timeout
import com.kinja.amqp.ignore
import com.kinja.amqp.model.{ Message, MessageConfirmation }
import org.slf4j.{ Logger => Slf4jLogger }

import scala.concurrent.duration._
import scala.concurrent.{ Await, ExecutionContext, Future, blocking }
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
	)(flushMemoryBufferToMessageStore())

	logger.debug("Memory flusher scheduled")

	override def saveConfirmations(confirms: List[MessageConfirmation]): Unit = {
		val (multiples, singles) = confirms.partition(_.multiple)
		// we don't save every multiple confirmation here,
		// just collect (and increment) them and save all at once in the flush loop
		inMemoryMessageBuffer ! MultipleConfirmations(multiples)
		messageStore.saveConfirmations(singles)
	}

	override def deleteMessage(id: Long): Unit = {
		messageStore.deleteMessage(id)
	}

	override def deleteOldSingleConfirms(olderThanSeconds: Long): Int = {
		messageStore.deleteOldSingleConfirms(olderThanSeconds)
	}

	override def lockRowsOlderThan(olderThanSeconds: Long, lockTimeOutAfterSeconds: Long, limit: Int): Int = {
		messageStore.lockRowsOlderThan(olderThanSeconds, lockTimeOutAfterSeconds, limit)
	}

	override def saveMessages(msgs: List[Message]): Unit = {
		inMemoryMessageBuffer ! SaveMessages(msgs)
	}

	override def deleteMultiConfIfNoMatchingMsg(olderThanSeconds: Long): Int = {
		messageStore.deleteMultiConfIfNoMatchingMsg(olderThanSeconds)
	}

	override def deleteMatchingMessagesAndSingleConfirms(): Int = {
		messageStore.deleteMatchingMessagesAndSingleConfirms()
	}

	override def deleteMessageUponConfirm(channelId: String, deliveryTag: Long): Future[Boolean] = {
		val matched: Future[Any] = inMemoryMessageBuffer ? DeleteMessageUponConfirm(channelId, deliveryTag)

		matched.flatMap {
			case false => messageStore.deleteMessageUponConfirm(channelId, deliveryTag)
			case _ => Future.successful(true)
		}
	}

	override def loadLockedMessages(limit: Int): List[Message] = {
		messageStore.loadLockedMessages(limit)
	}

	override def deleteMessagesWithMatchingMultiConfirms(): Int = {
		messageStore.deleteMessagesWithMatchingMultiConfirms()
	}

	private def flushMemoryBufferToMessageStore(): Unit = {
		tryWithLogging {
			logger.info("Flushing memory buffer to message store...")

			if (logger.isInfoEnabled) {
				inMemoryMessageBuffer ? LogBufferStatistics(logger)
			}

			handleMessagesResponseFromBuffer(
				inMemoryMessageBuffer ? RemoveMessagesOlderThan(memoryFlushInterval.toMillis)
			)

			handleConfirmationsResponseFromBuffer(
				inMemoryMessageBuffer ? RemoveMultipleConfirmations
			)
		}
	}

	private def tryWithLogging(f: => Unit): Unit = {
		try {
			f
		} catch {
			case NonFatal(t) => logger.error(
				s"[RabbitMQ] Exception while trying to flush in-memory buffer: ${t.getMessage}", t
			)
		}
	}

	private def handleMessagesResponseFromBuffer(response: Future[Any]): Unit = {
		blocking {
			val messagesSent: Future[Unit] = response map { messages =>
				logger.info(
					s"[${Thread.currentThread().getName}] Started flushing messages " +
						s"(${messages.asInstanceOf[List[Message]].size})..."
				)
				messages.asInstanceOf[List[Message]]
					.grouped(memoryFlushChunkSize)
					.foreach(group => {
						logger.info(s"[${Thread.currentThread().getName}] Flushing ${group.length} messages...")
						tryWithLogging(messageStore.saveMessages(group))
					})
				logger.info(s"[${Thread.currentThread().getName}] Finished flushing messages...")
			}
			Await.result(messagesSent, memoryFlushTimeOut)
		}
	}

	private def handleConfirmationsResponseFromBuffer(response: Future[Any]): Unit = {
		blocking {
			val confirmationsSent: Future[Unit] = response map { confirmations =>
				confirmations.asInstanceOf[List[MessageConfirmation]]
					.grouped(memoryFlushChunkSize)
					.foreach(group => {
						logger.info(s"Flushing ${group.length} confirmations...")
						tryWithLogging(messageStore.saveConfirmations(group))
					})
			}
			Await.result(confirmationsSent, memoryFlushTimeOut)
		}
	}

	override def shutdown(): Unit = {
		tryWithLogging {
			logger.info("Shutdown: flushing memory buffer to message store...")

			ignore(memoryFlushSchedule.cancel())

			if (logger.isInfoEnabled) {
				inMemoryMessageBuffer ? LogBufferStatistics(logger)
			}

			handleMessagesResponseFromBuffer(
				inMemoryMessageBuffer ? GetAllMessages
			)

			handleConfirmationsResponseFromBuffer(
				inMemoryMessageBuffer ? RemoveMultipleConfirmations
			)
		}
	}
}
