package com.kinja.amqp

import com.kinja.amqp.model.Message
import com.kinja.amqp.model.MessageConfirmation

trait MessageStore {

	def saveConfirmation(conf: MessageConfirmation): Unit
	def saveMessage(msg: Message): Unit

	/**
	 * Returns the number of deleted messages
	 */
	def deleteMessageUponConfirm(channelId: String, deliveryTag: Long): Int

	/**
	 * Within this method, the message store should make sure that
	 * the loaded records are not updated/deleted by any other transaction.
	 * If it can not be guaranteed AND multiple message repeater instances exist,
	 * same repeaters may try to resend or delete the very same messages/confirmations at a given time
	 */
	def withLockingTransaction[T](f: => T): T

	/**
	 * Used in withLockingTransaction
	 */
	def loadMessageOlderThan(time: Long, exchangeName: String, limit: Int): List[Message]

	/**
	 * Used in withLockingTransaction
	 */
	def loadConfirmationByChannels(channelIds: List[String]): List[MessageConfirmation]

	/**
	 * Used in withLockingTransaction
	 */
	def deleteMessage(id: Long): Unit

	/**
	 * Used in withLockingTransaction
	 */
	def deleteConfirmation(id: Long): Unit

}
