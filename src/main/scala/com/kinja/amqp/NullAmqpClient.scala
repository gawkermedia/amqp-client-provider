package com.kinja.amqp

import akka.actor.ActorRef

import scala.concurrent.Future

class NullAmqpClient extends AmqpClientInterface {
	override def getMessageProducer(exchangeName: String): AmqpProducerInterface = new NullAmqpProducer

	override def getMessageConsumer(queueName: String): AmqpConsumerInterface = new NullAmqpConsumer

	def startMessageRepeater(): Unit = {}

	def addConnectionListener(listener: ActorRef): Unit = {
		val _ = listener
	}

	override def shutdown(): Future[Unit] = Future.successful(())

	override def disconnect(): Unit = {}

	override def reconnect(): Unit = {}
}
