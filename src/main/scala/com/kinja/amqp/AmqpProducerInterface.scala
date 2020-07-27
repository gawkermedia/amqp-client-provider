package com.kinja.amqp

import java.util.UUID

import scala.concurrent.Future

trait AmqpProducerInterface {
	def publish[A: Writes](
		routingKey: String,
		message: A,
		messageId: UUID = UUID.randomUUID(),
		saveTimeMillis: Long = System.currentTimeMillis()
	): Future[Unit]

}
