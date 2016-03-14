package com.kinja.amqp

import com.kinja.amqp.exception.{ MissingConsumerException, MissingProducerException, MissingResendConfigException }
import com.kinja.amqp.persistence.MessageStore

import akka.actor.{ ActorRef, ActorSystem }
import com.github.sstone.amqp.Amqp.AddStatusListener
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

	private val producers: Map[String, AmqpProducerInterface] = createProducers()

	private val consumers: Map[String, AmqpConsumer] = createConsumers()

	@SuppressWarnings(Array("org.brianmckenna.wartremover.warts.Var"))
	private var repeater: Option[MessageBufferProcessor] = None

	def getMessageProducer(exchangeName: String): AmqpProducerInterface = {
		producers.getOrElse(exchangeName, throw new MissingProducerException(exchangeName))
	}

	def getMessageConsumer(queueName: String): AmqpConsumer = {
		consumers.getOrElse(queueName, throw new MissingConsumerException(queueName))
	}

	def startMessageRepeater() = {
		val conf = configuration.resendConfig.getOrElse(throw new MissingResendConfigException)
		repeater = Some(new MessageBufferProcessor(actorSystem, messageStore, producers, logger)(
			conf.initialDelayInSec,
			conf.bufferProcessInterval,
			conf.minMsgAge,
			conf.maxMultiConfirmAge,
			conf.maxSingleConfirmAge,
			conf.republishTimeoutInSec,
			conf.messageBatchSize,
			conf.messageLockTimeOutAfter
		))

		repeater.foreach(_.startSchedule(ec))
	}

	private def createProducers(): Map[String, AmqpProducerInterface] = {
		configuration.exchanges.map {
			case (name: String, producerConfig: ProducerConfig) =>
				val channelProvider = new ProducerChannelProvider(
					connection, actorSystem, configuration.connectionTimeOut, producerConfig.exchangeParams
				)
				val producer = producerConfig.deliveryGuarantee match {
					case DeliveryGuarantee.AtLeastOnce =>
						new AtLeastOnceAmqpProducer(
							producerConfig.exchangeParams.name,
							channelProvider,
							actorSystem,
							messageStore,
							configuration.askTimeOut,
							logger
						)(ec)
					case DeliveryGuarantee.AtMostOnce =>
						new AtMostOnceAmqpProducer(
							producerConfig.exchangeParams.name,
							channelProvider
						)(ec)
				}
				name -> producer
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

	override def shutdown(): Unit = {
		messageStore.shutdown()
		repeater.foreach(_.shutdown())
	}
}
