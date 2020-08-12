package com.kinja.amqp

import java.util.concurrent.atomic.AtomicLong

import com.kinja.amqp.model.FailedMessage
import com.kinja.amqp.persistence.MessageStore

import scala.concurrent.Future

class InMemoryMessageStore extends MessageStore {

	@SuppressWarnings(Array("org.wartremover.warts.Var"))
	var atomicLong: AtomicLong = new AtomicLong(0L)
	@SuppressWarnings(Array("org.wartremover.warts.Var"))
	var messages: Map[Long, (Boolean, FailedMessage)] = Map.empty[Long, (Boolean, FailedMessage)]
	/**
	 * Check if the storage has message to be process.
	 */
	override def hasMessageToProcess(): Future[Boolean] = {
		Future.successful(messages.filter { case (_, (false, _)) => true }.nonEmpty)
	}

	/**
	 * Save a list of messages to the storage
	 *
	 * @param msgs Messages to save
	 */
	override def saveFailedMessages(msgs: List[FailedMessage]): Future[Unit] = {
		messages = messages ++ msgs.map { msg =>
			val id = atomicLong.incrementAndGet()
			id -> ((false, msg.copy(id = Some(id))))
		}.toMap
		Future.successful(())
	}

	/**
	 * Delete a message that failed to be sent after resending it
	 *
	 * @param id ID of the message to be deleted
	 */
	override def deleteFailedMessage(id: Long): Future[Unit] = {
		messages -= id
		Future.successful(())
	}

	/**
	 * Do a cleanup, such as delete confirmed messages,
	 * as well as matching single confirmations and confirmations that are too old
	 *
	 * @return Whether there are messages to process
	 */
	override def cleanup(): Future[Boolean] = hasMessageToProcess()

	/**
	 * Lock and load an appropriate number of old messages
	 *
	 * @return List of locked messages
	 */
	override def lockAndLoad(): Future[List[FailedMessage]] = {
		val idsAndMessages = messages.filter { case (_, (false, _)) => true }.take(50)
		val lockedIdsAndMessages = idsAndMessages.map(x => x.copy(_2 = x._2.copy(_1 = true)))
		messages = (messages -- idsAndMessages.map(_._1)) ++ lockedIdsAndMessages
		Future.successful(lockedIdsAndMessages.map(_._2._2).toList)
	}

	/**
	 * Cleanup before shutting down the storage
	 */
	override def shutdown(): Future[Unit] = Future.successful(())
}
