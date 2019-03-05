package com.kinja.amqp.persistence

import com.kinja.amqp.model.MessageLike
import com.kinja.amqp.model.MessageConfirmation

import scala.concurrent.Future

trait MessageStore {

	/**
	 * Check if the storage has message to be process.
	 */
	def hasMessageToProcess(): Future[Boolean]

	/**
	 * Save a list of confirmations to the storage
	 *
	 * @param confirms Confirmations to save
	 */
	def saveConfirmations(confirms: List[MessageConfirmation]): Future[Unit]

	/**
	 * Save a list of messages to the storage
	 *
	 * @param msgs Messages to save
	 */
	def saveMessages(msgs: List[MessageLike]): Future[Unit]

	/**
	 * Delete message from the store, as it was confirmed or resent
	 *
	 * @param channelId ID of the channel the message was sent on
	 * @param deliveryTag ID of the message within that channel
	 * @return true if there was a message and it was deleted
	 */
	def deleteMessage(channelId: String, deliveryTag: Long): Future[Boolean]

	/**
	 * Delete a message that failed to be sent after resending it
	 *
	 * @param id ID of the message to be deleted
	 */
	def deleteFailedMessage(id: Long): Future[Unit]

	/**
	 * Delete single confirmations matching some messages, along with those messages
	 *
	 * @return Number of removed confirmations, which is also the number of removed messages
	 */
	def deleteMatchingMessagesAndSingleConfirms(): Future[Int]

	/**
	 * Delete messages that were confirmed by some multiple confirmation
	 *
	 * @return Number of messages deleted
	 */
	def deleteMessagesWithMatchingMultiConfirms(): Future[Int]

	/**
	 * Delete old multiple confirmations that do not match any messages
	 *
	 * @return Number of confirmations deleted
	 */
	def deleteMultiConfIfNoMatchingMsg(): Future[Int]

	/**
	 * Delete old single confirmations
	 *
	 * @return Number of confirmations deleted
	 */
	def deleteOldSingleConfirms(): Future[Int]

	/**
	 * Lock some messages to this host
	 *
	 * @param limit How many messages to lock
	 * @return Number of messages locked
	 */
	def lockOldRows(limit: Int): Future[Int]

	/**
	 * Load messages that were locked to this host
	 *
	 * @param limit How many messages to load
	 * @return List of locked messages
	 */
	def loadLockedMessages(limit: Int): Future[List[MessageLike]]

	/**
	 * Cleanup before shutting down the storage
	 */
	def shutdown(): Future[Unit]
}
