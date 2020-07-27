package com.kinja.amqp.persistence

import java.util.UUID

import com.kinja.amqp.model.FailedMessage

import scala.concurrent.Future

object NullMessageStore extends MessageStore {

	override def hasMessageToProcess(): Future[Boolean] = Future.successful(false)

	override def saveFailedMessages(msg: List[FailedMessage]): Future[Unit] = Future.successful(())

	override def cleanup(): Future[Boolean] = Future.successful(false)

	override def deleteMessage(messageId: UUID): Future[Boolean] = Future.successful(false)

	override def deleteFailedMessage(id: Long): Future[Unit] = Future.successful(())

	override def lockAndLoad(): Future[List[FailedMessage]] = Future.successful(List.empty[FailedMessage])

	override def shutdown(): Future[Unit] = Future.successful(())
}
