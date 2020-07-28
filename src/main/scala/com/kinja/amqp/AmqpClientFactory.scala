package com.kinja.amqp

import com.kinja.amqp.persistence.MessageStore
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.alpakka.amqp.{ AmqpConnectionFactoryConnectionProvider, AmqpConnectionProvider }
import com.kinja.amqp.configuration.{ AmqpConfiguration, AtLeastOnceGroup }
import com.rabbitmq.client.ConnectionFactory
import org.slf4j.Logger

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.control.NonFatal

class AmqpClientFactory {

	@SuppressWarnings(Array("org.wartremover.warts.Var"))
	private var clients: Set[AmqpConsumerClientInterface] = Set.empty[AmqpConsumerClientInterface]

	/**
	 * Create an AMQP Client which only provides API for consumer creation.
	 */
	def createConsumerClient(
		config: AmqpConfiguration,
		actorSystem: ActorSystem,
		materializer: Materializer,
		logger: Logger,
		ec: ExecutionContext
	): AmqpConsumerClientInterface =
		createClient(config, actorSystem, materializer, logger, ec, Map.empty[AtLeastOnceGroup, MessageStore])

	/**
	 * Create an AMQP Client which provides API for consumer and producer creation.
	 */
	def createClient(
		config: AmqpConfiguration,
		actorSystem: ActorSystem,
		materializer: Materializer,
		logger: Logger,
		ec: ExecutionContext,
		messageStore: (AtLeastOnceGroup, MessageStore)
	): AmqpClientInterface =
		createClient(config, actorSystem, materializer, logger, ec, Map(messageStore))

	/**
	 * Create an AMQP Client which provides API for consumer and producer creation.
	 */
	def createClient(
		config: AmqpConfiguration,
		actorSystem: ActorSystem,
		materializer: Materializer,
		logger: Logger,
		ec: ExecutionContext,
		messageStores: ::[(AtLeastOnceGroup, MessageStore)]
	): AmqpClientInterface =
		createClient(config, actorSystem, materializer, logger, ec, messageStores.toMap)

	private def createClient(
		config: AmqpConfiguration,
		actorSystem: ActorSystem,
		materializer: Materializer,
		logger: Logger,
		ec: ExecutionContext,
		messageStores: Map[AtLeastOnceGroup, MessageStore]
	): AmqpClientInterface =
		this.synchronized {
			if (config.testMode) {
				new NullAmqpClient
			} else {
				implicit val ex: ExecutionContext = ec
				val connection = createConnectionProvider(config)

				val client = new AmqpClient(
					connectionProvider = connection,
					actorSystem = actorSystem,
					materializer = materializer,
					configuration = config,
					logger = logger,
					messageStores = messageStores,
					ec = ec
				)
				ignore(Future {
					client.startMessageRepeater()
				}.recover {
					case NonFatal(e) =>
						logger.error("RabbitMQ message buffer processor failed to start: " + e.getMessage)
				})
				clients = clients + client
				client
			}
		}

	private def createConnectionProvider(config: AmqpConfiguration): AmqpConnectionProvider = {
		val factory = new ConnectionFactory()
		factory.setUsername(config.username)
		factory.setPassword(config.password)
		factory.setRequestedHeartbeat(config.heartbeatRate)
		factory.setConnectionTimeout(config.connectionTimeOut.toMillis.toInt)

		AmqpConnectionFactoryConnectionProvider(factory)
			.withHostsAndPorts(config.addresses.toList.map(a => (a.getHost(), a.getPort())))
	}

	/**
	 * Call disconnect on all created client.
	 */
	def disconnectAllClient(): Unit = clients.foreach(_.disconnect())

	/**
	 * Call reconnect on all created client.
	 */
	def reconnectAllClient(): Unit = clients.foreach(_.reconnect())

}
