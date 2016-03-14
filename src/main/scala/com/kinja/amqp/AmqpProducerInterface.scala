package com.kinja.amqp

import scala.concurrent.Future

trait AmqpProducerInterface {
	def publish[A: Writes](
		routingKey: String,
		message: A,
		saveTimeMillis: Long = System.currentTimeMillis()
	): Future[Unit]

}
