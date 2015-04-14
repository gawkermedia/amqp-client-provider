package com.kinja.amqp

import akka.actor.{ ActorRef, ActorSystem }
import com.github.sstone.amqp.Amqp.ExchangeParameters
import com.kinja.amqp.exception.{ MissingConsumerException, MissingProducerException }
import org.slf4j.{ Logger => Slf4jLogger }

trait AmqpClientProvider {

	protected val connection: ActorRef

	val actorSystem: ActorSystem

	protected val configuration: AmqpConfiguration

	protected val logger: Slf4jLogger

	protected val messageStore: MessageStore

	val producers: Map[String, AmqpProducer] = createProducers()

	val consumers: Map[String, AmqpConsumer] = createConsumers()

	def getMessageProducer(exchangeName: String): AmqpProducer = {
		producers.getOrElse(exchangeName, throw new MissingProducerException(exchangeName))
	}

	def getMessageConsumer(queueName: String): AmqpConsumer = {
		consumers.getOrElse(queueName, throw new MissingConsumerException(queueName))
	}

	private def createProducers(): Map[String, AmqpProducer] = {
		configuration.exchanges.map {
			case (name: String, params: ExchangeParameters) =>
				name -> new AmqpProducer(
					connection,
					actorSystem,
					messageStore,
					configuration.connectionTimeOut,
					configuration.askTimeOut,
					logger
				)(params)
		}
	}

	private def createConsumers(): Map[String, AmqpConsumer] = {
		configuration.queues.map {
			case (name: String, params: QueueWithRelatedParameters) =>
				name -> new AmqpConsumer(connection, actorSystem, configuration.connectionTimeOut, logger)(params)
		}
	}
}