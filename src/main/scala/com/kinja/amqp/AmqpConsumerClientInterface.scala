package com.kinja.amqp

import akka.actor.ActorRef

import scala.concurrent.Future

trait AmqpConsumerClientInterface {

	def getMessageConsumer(queueName: String): AmqpConsumerInterface

	def addConnectionListener(listener: ActorRef): Unit

	def shutdown(): Future[Unit]

	def disconnect(): Unit

	def reconnect(): Unit
}
