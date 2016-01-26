package com.kinja.amqp

import scala.concurrent.Future

class NullAmqpProducer extends AmqpProducerInterface {
	override def publish[A: Writes](
		routingKey: String,
		message: A,
		saveTimeMillis: Long = System.currentTimeMillis()
	): Future[Unit] = Future.successful(())
}
