package com.kinja.amqp.persistence

import com.kinja.amqp.model.Message
import com.kinja.amqp.model.MessageConfirmation

trait MessageStore {

	def saveConfirmation(conf: MessageConfirmation): Unit

	def saveMessage(msg: Message): Unit

	/**
	 * Returns the number of deleted messages
	 */
	def deleteMessageUponConfirm(channelId: String, deliveryTag: Long): Int

	def deleteMatchingMessagesAndSingleConfirms(): Unit

	def deleteMessagesWithMatchingMultiConfirms(): Unit

	def deleteMultiConfIfNoMatchingMsg(olderThanSeconds: Long): Unit

	def deleteOldSingleConfirms(olderThanSeconds: Long): Unit

	def lockRowsOlderThan(olderThanSeconds: Long, lockTimeOutAfterSeconds: Long, limit: Int): Unit

	def loadLockedMessages(limit: Int): List[Message]

	def deleteMessage(id: Long): Unit
}
