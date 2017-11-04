package com.kinja.amqp.persistence

import com.kinja.amqp.model.{ Message, MessageConfirmation }

import scala.concurrent.Future

object NullMessageStore extends MessageStore {

	override def saveMessages(msg: List[Message]): Unit = {}

	override def saveConfirmations(confirms: List[MessageConfirmation]): Unit = {}

	override def deleteMessageUponConfirm(
		channelId: String,
		deliveryTag: Long
	): Future[Boolean] = Future.successful(false)

	override def deleteMultiConfIfNoMatchingMsg(): Int = 0

	override def deleteMatchingMessagesAndSingleConfirms(): Int = 0

	override def deleteMessage(id: Long): Unit = {}

	override def lockOldRows(limit: Int): Int = 0

	override def deleteOldSingleConfirms(): Int = 0

	override def loadLockedMessages(limit: Int): List[Message] = List.empty[Message]

	override def deleteMessagesWithMatchingMultiConfirms(): Int = 0

	override def shutdown(): Unit = {}
}
