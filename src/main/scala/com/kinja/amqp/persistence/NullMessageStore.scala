package com.kinja.amqp.persistence

import com.kinja.amqp.model.{ Message, MessageConfirmation }

import scala.concurrent.Future

object NullMessageStore extends MessageStore {

	override def saveMessage(msg: Message): Unit = {}

	override def saveConfirmation(confirm: MessageConfirmation): Unit = {}

	override def deleteMessageUponConfirm(
		channelId: String,
		deliveryTag: Long
	): Future[Boolean] = Future.successful(false)

	override def deleteMultiConfIfNoMatchingMsg(olderThan: Long): Int = 0

	override def deleteMatchingMessagesAndSingleConfirms(): Int = 0

	override def deleteMessage(id: Long): Unit = {}

	override def lockRowsOlderThan(olderThanSeconds: Long, lockTimeOutAfterSeconds: Long, limit: Int): Int = 0

	override def deleteOldSingleConfirms(olderThanSeconds: Long): Int = 0

	override def loadLockedMessages(limit: Int): List[Message] = List.empty[Message]

	override def deleteMessagesWithMatchingMultiConfirms(): Int = 0
}
