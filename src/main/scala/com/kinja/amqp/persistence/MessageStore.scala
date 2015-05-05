package com.kinja.amqp.persistence

import com.kinja.amqp.TransactionalMessageStore
import com.kinja.amqp.model.Message
import com.kinja.amqp.model.MessageConfirmation

trait MessageStore {

	def saveConfirmation(conf: MessageConfirmation): Unit

	def saveMessage(msg: Message): Unit

	/**
	 * Returns the number of deleted messages
	 */
	def deleteMessageUponConfirm(channelId: String, deliveryTag: Long): Int

	def createTransactionalStore: TransactionalMessageStore

	def deleteMultiConfIfNoMatchingMsg(olderThan: Long): Unit

}