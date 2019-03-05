package com.kinja.amqp.persistence

import com.kinja.amqp.model.{ Message, MessageConfirmation, MessageLike }

import scala.concurrent.Future

object NullMessageStore extends MessageStore {

	override def hasMessageToProcess(): Future[Boolean] = Future.successful(false)

	override def saveMessages(msg: List[MessageLike]): Future[Unit] = Future.successful(())

	override def saveConfirmations(confirms: List[MessageConfirmation]): Future[Unit] = Future.successful(())

	override def deleteMessage(channelId: String, deliveryTag: Long): Future[Boolean] = Future.successful(false)

	override def deleteMultiConfIfNoMatchingMsg(): Future[Int] = Future.successful(0)

	override def deleteMatchingMessagesAndSingleConfirms(): Future[Int] = Future.successful(0)

	override def deleteFailedMessage(id: Long): Future[Unit] = Future.successful(())

	override def lockOldRows(limit: Int): Future[Int] = Future.successful(0)

	override def deleteOldSingleConfirms(): Future[Int] = Future.successful(0)

	override def loadLockedMessages(limit: Int): Future[List[MessageLike]] = Future.successful(List.empty[Message])

	override def deleteMessagesWithMatchingMultiConfirms(): Future[Int] = Future.successful(0)

	override def shutdown(): Future[Unit] = Future.successful(())
}
