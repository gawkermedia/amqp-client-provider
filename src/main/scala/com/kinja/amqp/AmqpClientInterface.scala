package com.kinja.amqp

import akka.actor.ActorRef

trait AmqpClientInterface {
	def getMessageProducer(exchangeName: String): AmqpProducerInterface

	def getMessageConsumer(queueName: String): AmqpConsumerInterface

	def addConnectionListener(listener: ActorRef): Unit

	def startMessageRepeater(): Unit

	def shutdown(): Unit

	def disconnect(message: String, timeout: Int = -1): Unit
}
