package com.kinja.amqp.persistence

import akka.actor.{ ActorRef, ActorSystem, Props }
import akka.pattern.ask
import akka.util.Timeout
import com.kinja.amqp.model.{ Message, MessageConfirmation }
import org.slf4j.{ Logger => Slf4jLogger }

import scala.concurrent.duration._
import scala.concurrent.{ Await, ExecutionContext, Future, blocking }
import scala.util.control.NonFatal

class InMemoryMessageBufferDecorator(
	messageStore: MySqlMessageStore,
	actorSystem: ActorSystem,
	logger: Slf4jLogger,
	memoryFlushInterval: FiniteDuration,
	memoryFlushChunkSize: Int,
	memoryFlushTimeOut: FiniteDuration,
	askTimeout: FiniteDuration
)(implicit val ec: ExecutionContext) extends MessageStore {

	private implicit val timeout = Timeout(askTimeout)

	private val inMemoryMessageBuffer: ActorRef = actorSystem.actorOf(Props(new InMemoryMessageBuffer))

	logger.debug("Scheduling memory flusher...")

	actorSystem.scheduler.schedule(1 second, memoryFlushInterval)(flushMemoryBufferToMessageStore())

	logger.debug("Memory flusher scheduled")

	override def saveConfirmation(confirm: MessageConfirmation): Unit = {
		if (confirm.multiple) {
			// we don't save every multiple confirmation here,
			// just collect (and increment) them and save all at once in the flush loop
			inMemoryMessageBuffer ! MultipleConfirmation(confirm)
		} else {
			messageStore.saveConfirmation(confirm)
		}
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

	override def saveMessage(msg: Message): Unit = {
		inMemoryMessageBuffer ! SaveMessage(msg)
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

			val messages: Future[Any] = inMemoryMessageBuffer ? RemoveMessagesOlderThan(memoryFlushInterval.toMillis)

			val messagesSent: Future[Unit] = messages map { msgs =>
				blocking {
					logger.info(
						s"[${Thread.currentThread().getName}] Started flushing messages " +
							s"(${msgs.asInstanceOf[List[Message]].size})..."
					)
					msgs.asInstanceOf[List[Message]]
						.grouped(memoryFlushChunkSize)
						.foreach(group => {
							logger.info(s"[${Thread.currentThread().getName}] Flushing ${group.length} messages...")
							tryWithLogging(messageStore.saveMultipleMessages(group))
						})
					logger.info(s"[${Thread.currentThread().getName}]Finished flushing messages...")
				}
			}

			Await.result(messagesSent, memoryFlushTimeOut)

			val confirmations: Future[Any] = inMemoryMessageBuffer ? RemoveMultipleConfirmations

			val confirmationsSent: Future[Unit] = confirmations map { confirms =>
				blocking {
					confirms.asInstanceOf[List[MessageConfirmation]]
						.grouped(memoryFlushChunkSize)
						.foreach(group => {
							logger.info(s"Flushing ${group.length} confirmations...")
							tryWithLogging(messageStore.saveMultipleConfirmations(group))
						})
				}
			}

			Await.result(confirmationsSent, memoryFlushTimeOut)
		}
	}

	private def tryWithLogging(f: => Unit): Unit = {
		try {
			f
		} catch {
			case NonFatal(t) => logger.error(
				s"[RabbitMQ] Exception while trying to flush in-memory buffer: $t,\n Trace:${t.getStackTraceString}"
			)
		}
	}
}
