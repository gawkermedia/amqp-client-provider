package com.kinja.amqp

import java.util.UUID

import scala.concurrent.Future

class NullAmqpProducer extends AmqpProducerInterface {
	override def publish[A: Writes](
		routingKey: String,
		message: A,
		saveTimeMillis: Long = System.currentTimeMillis()
	): Future[Unit] = Future.successful(())

	override def publish[A: Writes](
		routingKey: String,
		message: A,
		messageId: UUID,
		saveTimeMillis: Long
	): Future[Unit] = Future.successful(())
}
