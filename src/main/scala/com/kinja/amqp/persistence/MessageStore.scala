package com.kinja.amqp.persistence

import com.kinja.amqp.model.Message
import com.kinja.amqp.model.MessageConfirmation

import scala.concurrent.Future

trait MessageStore {

	/**
	 * Save a list of confirmations to the storage
	 *
	 * @param confirms Confirmations to save
	 */
	def saveConfirmations(confirms: List[MessageConfirmation]): Unit

	/**
	 * Save a list of messages to the storage
	 *
	 * @param msgs Messages to save
	 */
	def saveMessages(msgs: List[Message]): Unit

	/**
	 * Delete message from the store, as it was confirmed
	 *
	 * @param channelId ID of the channel the message was sent on
	 * @param deliveryTag ID of the message within that channel
	 * @return true if there was a message and it was deleted
	 */
	def deleteMessageUponConfirm(channelId: String, deliveryTag: Long): Future[Boolean]

	/**
	 * Delete single confirmations matching some messages, along with those messages
	 *
	 * @return Number of removed confirmations, which is also the number of removed messages
	 */
	def deleteMatchingMessagesAndSingleConfirms(): Int

	/**
	 * Delete messages that were confirmed by some multiple confirmation
	 *
	 * @return Number of messages deleted
	 */
	def deleteMessagesWithMatchingMultiConfirms(): Int

	/**
	 * Delete old multiple confirmations that do not match any messages
	 *
	 * @param olderThanSeconds How old a confirmation should be to be deleted
	 * @return Number of confirmations deleted
	 */
	def deleteMultiConfIfNoMatchingMsg(olderThanSeconds: Long): Int

	/**
	 * Delete old single confirmations
	 *
	 * @param olderThanSeconds How old a confirmation should be to be deleted
	 * @return Number of confirmations deleted
	 */
	def deleteOldSingleConfirms(olderThanSeconds: Long): Int

	/**
	 * Lock some messages to this host
	 *
	 * @param olderThanSeconds How old a message should be to be locked
	 * @param lockTimeOutAfterSeconds How long ago a message should be locked by some other host to be relocked
	 * @param limit How many messages to lock
	 * @return Number of messages locked
	 */
	def lockRowsOlderThan(olderThanSeconds: Long, lockTimeOutAfterSeconds: Long, limit: Int): Int

	/**
	 * Load messages that were locked to this host
	 *
	 * @param limit How many messages to load
	 * @return List of locked messages
	 */
	def loadLockedMessages(limit: Int): List[Message]

	/**
	 * Delete specific message
	 *
	 * @param id ID of the message to be deleted
	 */
	def deleteMessage(id: Long): Unit

	/**
	 * Cleanup before shutting down the storage
	 */
	def shutdown(): Unit
}
