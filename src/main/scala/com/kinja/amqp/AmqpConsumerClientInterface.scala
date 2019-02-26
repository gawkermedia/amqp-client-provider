package com.kinja.amqp

import scala.concurrent.Future

trait AmqpConsumerClientInterface {

	def getMessageConsumer(queueName: String): AmqpConsumerInterface

	def shutdown(): Future[Unit]

	def disconnect(): Unit

	def reconnect(): Unit
}
