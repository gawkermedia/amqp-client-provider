package com.kinja.amqp.persistence

import com.kinja.amqp.model.Message
import com.kinja.amqp.model.MessageConfirmation

import scala.concurrent.Future

trait MessageStore {

	def saveConfirmation(confirm: MessageConfirmation): Unit

	def saveMessage(msg: Message): Unit

	/**
	 * Returns if there was a message deleted
	 */
	def deleteMessageUponConfirm(channelId: String, deliveryTag: Long): Future[Boolean]

	def deleteMatchingMessagesAndSingleConfirms(): Int

	def deleteMessagesWithMatchingMultiConfirms(): Int

	def deleteMultiConfIfNoMatchingMsg(olderThanSeconds: Long): Int

	def deleteOldSingleConfirms(olderThanSeconds: Long): Int

	def lockRowsOlderThan(olderThanSeconds: Long, lockTimeOutAfterSeconds: Long, limit: Int): Int

	def loadLockedMessages(limit: Int): List[Message]

	def deleteMessage(id: Long): Unit

	def shutdown(): Unit
}
