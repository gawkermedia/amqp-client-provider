package com.kinja.amqp

import java.util.UUID

import com.kinja.amqp.model.Message
import com.kinja.amqp.model.MessageConfirmation

trait MessageStore {

	/**
	 * Between start and commit, the message store should make sure that
	 * the loaded records are not updated/deleted by any other transaction.
	 * If it can not be guaranteed AND multiple message repeater instances exist,
	 * same repeaters may try to resend or delete the very same messages/confirmations at a given time
	 */
	def startTransaction(): Unit
	def commit(): Unit
	def saveConfirmation(conf: MessageConfirmation): Unit
	def saveMessage(msg: Message): Unit
	def loadMessageOlderThan(time: Long, exchangeName: String, limit: Int): List[Message]
	def loadConfirmationByChannels(channelIds: List[UUID]): List[MessageConfirmation]
	def deleteMessage(id: Long): Unit

	/**
	 * Returns the number of deleted messages
	 */
	def deleteMessageUponConfirm(channelId: UUID, deliveryTag: Long): Int
	def deleteConfirmation(id: Long): Unit
}
