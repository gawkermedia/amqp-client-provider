package com.kinja.amqp

import scala.concurrent.Future

import java.util.UUID

class NullAmqpProducer extends AmqpProducerInterface {
	override def publish[A: Writes](
		routingKey: String,
		message: A,
		messageId: UUID = UUID.randomUUID(),
		saveTimeMillis: Long = System.currentTimeMillis()
	): Future[Unit] = Future.successful(())
}
