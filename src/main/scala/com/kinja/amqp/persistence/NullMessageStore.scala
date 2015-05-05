package com.kinja.amqp.persistence

import com.kinja.amqp.TransactionalMessageStore
import com.kinja.amqp.model.Message
import com.kinja.amqp.model.MessageConfirmation
import com.kinja.amqp.persistence.MessageStore

object NullMessageStore extends MessageStore {

	override def saveMessage(msg: Message): Unit = {}

	override def saveConfirmation(conf: MessageConfirmation): Unit = {}

	override def deleteMessageUponConfirm(channelId: String, deliveryTag: Long): Int = 0

	override def createTransactionalStore(): TransactionalMessageStore = new NullTransactionalStore

	override def deleteMultiConfIfNoMatchingMsg(olderThan: Long): Unit = {}

	class NullTransactionalStore extends TransactionalMessageStore {
		override def start: Unit = {}
		override def commit: Unit = {}
		override def loadMessageOlderThan(time: Long, exchangeName: String, limit: Int): List[Message] = List()
		override def loadConfirmationByChannels(channelIds: List[String]): List[MessageConfirmation] = List()
		override def deleteMessage(id: Long): Unit = {}
		override def deleteConfirmation(id: Long): Unit = {}
	}

}
