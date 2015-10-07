package com.kinja.amqp

import com.kinja.amqp.exception.{ MissingConsumerException, MissingProducerException, MissingResendConfigException }
import com.kinja.amqp.persistence.MessageStore

import akka.actor.{ ActorRef, ActorSystem }
import com.github.sstone.amqp.Amqp.{ AddStatusListener, ExchangeParameters }
import org.slf4j.{ Logger => Slf4jLogger }
import scala.concurrent.ExecutionContext

class AmqpClient(
	private val connection: ActorRef,
	val actorSystem: ActorSystem,
	private val configuration: AmqpConfiguration,
	private val logger: Slf4jLogger,
	private val messageStore: MessageStore,
	private val ec: ExecutionContext
) extends AmqpClientInterface {

	private val producers: Map[String, AmqpProducer] = createProducers()

	private val consumers: Map[String, AmqpConsumer] = createConsumers()

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
			conf.bufferProcessInterval,
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
					logger
				)(params, ec)
		}
	}

	private def createConsumers(): Map[String, AmqpConsumer] = {
		configuration.queues.map {
			case (name: String, params: QueueWithRelatedParameters) =>
				name -> new AmqpConsumer(
					connection,
					actorSystem,
					configuration.connectionTimeOut,
					logger
				)(params)
		}
	}

	override def addConnectionListener(listener: ActorRef): Unit = connection ! AddStatusListener(listener)

	override def shutdown(): Unit = messageStore.shutdown()
}