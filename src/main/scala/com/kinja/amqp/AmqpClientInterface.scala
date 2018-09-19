package com.kinja.amqp

import akka.actor.ActorRef

import scala.concurrent.Future

trait AmqpClientInterface {
	def getMessageProducer(exchangeName: String): AmqpProducerInterface

	def getMessageConsumer(queueName: String): AmqpConsumerInterface

	def addConnectionListener(listener: ActorRef): Unit

	def startMessageRepeater(): Unit

	def shutdown(): Future[Unit]

	def disconnect(): Unit

	def reconnect(): Unit
}
