package com.kinja.amqp.persistence

import com.kinja.amqp.model.{ MessageConfirmation, MessageLike }

import scala.concurrent.Future

object NullMessageStore extends MessageStore {

	override def hasMessageToProcess(): Future[Boolean] = Future.successful(false)

	override def saveMessages(msg: List[MessageLike]): Future[Unit] = Future.successful(())

	override def saveConfirmations(confirms: List[MessageConfirmation]): Future[Unit] = Future.successful(())

	override def cleanup(): Future[Boolean] = Future.successful(false)

	override def deleteMessage(channelId: String, deliveryTag: Long): Future[Boolean] = Future.successful(false)

	override def deleteFailedMessage(id: Long): Future[Unit] = Future.successful(())

	override def lockAndLoad(): Future[List[MessageLike]] = Future.successful(List.empty[MessageLike])

	override def shutdown(): Future[Unit] = Future.successful(())
}
