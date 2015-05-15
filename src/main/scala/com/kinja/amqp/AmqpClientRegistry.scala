package com.kinja.amqp

import akka.actor.{ ActorRef, ActorSystem }
import com.github.sstone.amqp.Amqp.ExchangeParameters
import com.kinja.amqp.exception.{ MissingResendConfigException, MissingConsumerException, MissingProducerException }
import com.kinja.amqp.persistence.MessageStore
import org.slf4j.{ Logger => Slf4jLogger }

import scala.concurrent.ExecutionContext

trait AmqpClientRegistry {

	protected val connection: ActorRef

	val actorSystem: ActorSystem

	protected val configuration: AmqpConfiguration

	protected val logger: Slf4jLogger

	protected val messageStore: MessageStore

	protected val ec: ExecutionContext

	val producers: Map[String, AmqpProducer] = createProducers()

	val consumers: Map[String, AmqpConsumer] = createConsumers()

	def getMessageProducer(exchangeName: String): AmqpProducer = {
		producers.getOrElse(exchangeName, throw new MissingProducerException(exchangeName))
	}

	def getMessageConsumer(queueName: String): AmqpConsumer = {
		consumers.getOrElse(queueName, throw new MissingConsumerException(queueName))
	}

	def startMessageRepeater() = {
		val conf = configuration.resendConfig.getOrElse(throw new MissingResendConfigException)
		val repeater = new MessageBufferProcessor(actorSystem, messageStore, producers, logger)(
			conf.initialDelayInSec,
			conf.interval,
			conf.minMsgAge,
			conf.maxMultiConfirmAge,
			conf.maxSingleConfirmAge,
			conf.republishTimeoutInSec,
			conf.messageBatchSize,
			conf.messageLockTimeOutAfter
		)

		repeater.startSchedule(ec)
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
					logger,
					ec
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