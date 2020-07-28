package com.kinja.amqp

import com.kinja.amqp.exception.{ MissingConsumerException, MissingProducerException, MissingResendConfigException }
import com.kinja.amqp.persistence.MessageStore
import com.kinja.amqp.impl.akkastream.{ AmqpConsumer, AtLeastOnceAmqpProducer, AtMostOnceAmqpProducer, ConsumerConfig }
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.alpakka.amqp.AmqpConnectionProvider
import com.kinja.amqp.configuration.{ AmqpConfiguration, AtLeastOnceGroup }
import org.slf4j.{ Logger => Slf4jLogger }

import scala.concurrent.{ ExecutionContext, Future }

class AmqpClient(
	private val connectionProvider: AmqpConnectionProvider,
	val actorSystem: ActorSystem,
	private val materializer: Materializer,
	private val configuration: AmqpConfiguration,
	private val logger: Slf4jLogger,
	private val messageStores: Map[AtLeastOnceGroup, MessageStore],
	private val ec: ExecutionContext
) extends AmqpClientInterface {

	private val producers: Map[String, AmqpProducerInterface with WithShutdown] = createProducers()

	private val consumers: Map[String, AmqpConsumerInterface with WithShutdown] = createConsumers()

	private lazy val repeater: List[MessageBufferProcessor] =
		messageStores.toList.map {
			case (groupName, messageStore) =>
				val conf = configuration.resendConfig.getOrElse(throw new MissingResendConfigException)
				val selectedProducers = producers.filter {
					case (exchangeName, _) =>
						configuration.exchanges
							.get(exchangeName)
							.map(_.atLeastOnceGroup)
							.contains(groupName)
				}
				new MessageBufferProcessor(
					actorSystem,
					messageStore,
					selectedProducers,
					logger
				)(
					conf.initialDelayInSec,
					conf.bufferProcessInterval,
					conf.republishTimeoutInSec
				)

		}

	override def getMessageProducer(exchangeName: String): AmqpProducerInterface = {
		producers.getOrElse(exchangeName, throw new MissingProducerException(exchangeName))
	}

	override def getMessageConsumer(queueName: String): AmqpConsumerInterface = {
		consumers.getOrElse(queueName, throw new MissingConsumerException(queueName))
	}

	def startMessageRepeater(): Unit = {
		repeater.foreach(_.startSchedule(ec))
	}

	private def createProducers(): Map[String, AmqpProducerInterface with WithShutdown] = {
		configuration.exchanges.map {
			case (name, producerConfig) =>
				name ->
					(messageStores.get(producerConfig.atLeastOnceGroup) match {
						case Some(messageStore) =>
							new AtLeastOnceAmqpProducer(
								connectionProvider = connectionProvider,
								logger = logger,
								system = actorSystem,
								materializer = materializer,
								params = producerConfig.exchangeParams,
								messageStore = messageStore
							)
						case None =>
							new AtMostOnceAmqpProducer(
								connectionProvider = connectionProvider,
								logger = logger,
								system = actorSystem,
								materializer = materializer,
								params = producerConfig.exchangeParams
							)
					})
		}
	}

	private def createConsumers(): Map[String, AmqpConsumerInterface with WithShutdown] = {
		configuration.queues.map {
			case (name, params) =>
				name -> new AmqpConsumer(
					connectionProvider = connectionProvider,
					system = actorSystem,
					materializer = materializer,
					logger = logger,
					consumerConfig = ConsumerConfig(
						connectionTimeOut = configuration.connectionTimeOut,
						shutdownTimeout = configuration.shutdownTimeOut,
						reconnectionTime = configuration.reconnectionTime,
						defaultPrefetchCount = configuration.defaultPrefetchCount.getOrElse(10),
						defaultParallelism = configuration.maxParallelism,
						defaultThrottling = configuration.throttling,
						defaultProcessingTimeout = configuration.processingTimeOut
					),
				)(params)
		}
	}

	override def shutdown(): Future[Unit] = {
		implicit val ex: ExecutionContext = actorSystem.dispatcher
		for {
			_ <- Future.sequence(producers.values.map(p => p.shutdown))
			_ <- Future.sequence(messageStores.values.map(_.shutdown()))
				.map(_ => ignore(repeater.foreach(_.shutdown())))
		} yield ()
	}

	override def disconnect(): Unit = {
		repeater.foreach(_.stopLocking(ec))
		consumers.foreach { case (_, consumer) => consumer.disconnect() }
	}

	override def reconnect(): Unit = {
		consumers.foreach { case (_, consumer) => consumer.reconnect() }
		repeater.foreach(_.resumeLocking(ec))
	}
}
