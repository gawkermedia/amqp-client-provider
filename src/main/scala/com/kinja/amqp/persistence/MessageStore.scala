package com.kinja.amqp.persistence

import com.kinja.amqp.model.FailedMessage

import scala.concurrent.Future

trait MessageStore {

	/**
	 * Check if the storage has message to be process.
	 */
	def hasMessageToProcess(): Future[Boolean]

	/**
	 * Save a list of messages to the storage
	 *
	 * @param msgs Messages to save
	 */
	def saveFailedMessages(msgs: List[FailedMessage]): Future[Unit]

	/**
	 * Delete a message that failed to be sent after resending it
	 *
	 * @param id ID of the message to be deleted
	 */
	def deleteFailedMessage(id: Long): Future[Unit]

	/**
	 * Do a cleanup, such as delete confirmed messages,
	 * as well as matching single confirmations and confirmations that are too old
	 *
	 * @return Whether there are messages to process
	 */
	def cleanup(): Future[Boolean]

	/**
	 * Lock and load an appropriate number of old messages
	 *
	 * @return List of locked messages
	 */
	def lockAndLoad(): Future[List[FailedMessage]]

	/**
	 * Cleanup before shutting down the storage
	 */
	def shutdown(): Future[Unit]
}
