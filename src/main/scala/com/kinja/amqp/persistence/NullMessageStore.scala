package com.kinja.amqp.persistence

import com.kinja.amqp.model.{ Message, MessageConfirmation }

object NullMessageStore extends MessageStore {

	override def saveMessage(msg: Message): Unit = {}

	override def saveConfirmation(conf: MessageConfirmation): Unit = {}

	override def deleteMessageUponConfirm(channelId: String, deliveryTag: Long): Int = 0

	override def deleteMultiConfIfNoMatchingMsg(olderThan: Long): Unit = {}

	override def deleteMatchingMessagesAndSingleConfirms(): Unit = {}

	override def deleteMessage(id: Long): Unit = {}

	override def lockRowsOlderThan(olderThanSeconds: Long, lockTimeOutAfterSeconds: Long, limit: Int): Unit = {}

	override def deleteOldSingleConfirms(olderThanSeconds: Long): Unit = {}

	override def loadLockedMessages(limit: Int): List[Message] = List.empty[Message]

	override def deleteMessagesWithMatchingMultiConfirms(): Unit = {}
}
