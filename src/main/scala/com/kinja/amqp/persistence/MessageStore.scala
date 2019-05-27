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
	 * Delete confirmed messages, as well as matching single confirmations
	 * and confirmations that are too old
	 *
	 * @return Number of removed messages
	 */
	def cleanup(): Future[Boolean]

	/**
	 * Lock and load an appropriate number of old messages
	 *
	 * @return List of locked messages
	 */
	def lockAndLoad(): Future[List[MessageLike]]

	/**
	 * Cleanup before shutting down the storage
	 */
	def shutdown(): Future[Unit]
}
