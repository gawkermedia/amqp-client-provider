package com.kinja.amqp

import com.kinja.amqp.exception.{ MissingConsumerException, MissingProducerException, MissingResendConfigException }
import com.kinja.amqp.persistence.MessageStore
import akka.actor.{ ActorRef, ActorSystem }
import com.github.sstone.amqp.Amqp.AddStatusListener
import org.slf4j.{ Logger => Slf4jLogger }

import scala.concurrent.{ ExecutionContext, Future }

class AmqpClient(
	private val connection: ActorRef,
	val actorSystem: ActorSystem,
	private val configuration: AmqpConfiguration,
	private val logger: Slf4jLogger,
	private val messageStores: Map[AtLeastOnceGroup, MessageStore],
	private val ec: ExecutionContext
) extends AmqpClientInterface {

	private val producers: Map[String, AmqpProducerInterface] = createProducers()

	private val consumers: Map[String, AmqpConsumer] = createConsumers()

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

	override def getMessageConsumer(queueName: String): AmqpConsumer = {
		consumers.getOrElse(queueName, throw new MissingConsumerException(queueName))
	}

	def startMessageRepeater(): Unit = {
		repeater.foreach(_.startSchedule(ec))
	}

	private def createProducers(): Map[String, AmqpProducerInterface] = {
		configuration.exchanges.map {
			case (name, producerConfig) =>
				val channelProvider = new ProducerChannelProvider(
					connection, actorSystem, configuration.connectionTimeOut, producerConfig.exchangeParams
				)
				name ->
					(messageStores.get(producerConfig.atLeastOnceGroup) match {
						case Some(messageStore) =>
							new AtLeastOnceAmqpProducer(
								producerConfig.exchangeParams.name,
								channelProvider,
								actorSystem,
								messageStore,
								configuration.askTimeOut,
								logger
							)(ec)
						case None =>
							new AtMostOnceAmqpProducer(
								producerConfig.exchangeParams.name,
								channelProvider
							)(ec)
					})
		}
	}

	private def createConsumers(): Map[String, AmqpConsumer] = {
		configuration.queues.map {
			case (name, params) =>
				name -> new AmqpConsumer(
					connection,
					actorSystem,
					configuration.connectionTimeOut,
					configuration.defaultPrefetchCount,
					logger
				)(params)
		}
	}

	def addConnectionListener(listener: ActorRef): Unit = connection ! AddStatusListener(listener)

	override def shutdown(): Future[Unit] = {
		implicit val ex: ExecutionContext = actorSystem.dispatcher
		Future.sequence(messageStores.values.map(_.shutdown()))
			.map(_ => ignore(repeater.foreach(_.shutdown())))
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
