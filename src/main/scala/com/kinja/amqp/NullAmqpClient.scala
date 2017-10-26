package com.kinja.amqp

import akka.actor.ActorRef

class NullAmqpClient extends AmqpClientInterface {
	override def getMessageProducer(exchangeName: String): AmqpProducerInterface = new NullAmqpProducer

	override def getMessageConsumer(queueName: String): AmqpConsumerInterface = new NullAmqpConsumer

	override def startMessageRepeater(): Unit = {}

	override def addConnectionListener(listener: ActorRef): Unit = {}

	override def shutdown(): Unit = {}

	override def disconnect(message: String, timeout: Int): Unit = {}
}
