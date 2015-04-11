package com.kinja.amqp

import akka.actor.ActorRef
import akka.actor.ActorSystem

import org.slf4j.{ Logger => Slf4jLogger }

trait AmqpClientProvider {

	protected val connection: ActorRef

	protected val actorSystem: ActorSystem

	protected val configuration: AmqpConfiguration

	protected val logger: Slf4jLogger

	protected val messageStore: MessageStore

	def createMessageProducer(exchangeName: String): AmqpProducer = {
		val exchangeParams = configuration.getExchangeParams(exchangeName)

		new AmqpProducer(connection, actorSystem, messageStore, configuration.connectionTimeOut, configuration.askTimeOut, logger)(exchangeParams)
	}

	def createMessageConsumer(queueName: String): AmqpConsumer = {
		val queueParameters = configuration.getQueueParams(queueName)

		new AmqpConsumer(connection, actorSystem, configuration.connectionTimeOut, logger)(queueParameters)
	}
}