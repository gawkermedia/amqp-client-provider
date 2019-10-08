package com.kinja.amqp

import com.kinja.amqp.persistence.{ InMemoryMessageBufferDecorator, MessageStore }
import akka.actor.{ ActorRef, ActorSystem }
import com.github.sstone.amqp.ConnectionOwner
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
		logger: Logger,
		ec: ExecutionContext,
		connectionListener: Option[ActorRef]
	): AmqpConsumerClientInterface =
		createClient(config, actorSystem, logger, ec, Map.empty[AtLeastOnceGroup, MessageStore], connectionListener)

	/**
	 * Create an AMQP Client which provides API for consumer and producer creation.
	 */
	def createClient(
		config: AmqpConfiguration,
		actorSystem: ActorSystem,
		logger: Logger,
		ec: ExecutionContext,
		messageStore: (AtLeastOnceGroup, MessageStore),
		connectionListener: Option[ActorRef]
	): AmqpClientInterface =
		createClient(config, actorSystem, logger, ec, Map(messageStore), connectionListener)

	/**
	 * Create an AMQP Client which provides API for consumer and producer creation.
	 */
	def createClient(
		config: AmqpConfiguration,
		actorSystem: ActorSystem,
		logger: Logger,
		ec: ExecutionContext,
		messageStores: ::[(AtLeastOnceGroup, MessageStore)],
		connectionListener: Option[ActorRef]
	): AmqpClientInterface =
		createClient(config, actorSystem, logger, ec, messageStores.toMap, connectionListener)

	private def createClient(
		config: AmqpConfiguration,
		actorSystem: ActorSystem,
		logger: Logger,
		ec: ExecutionContext,
		messageStores: Map[AtLeastOnceGroup, MessageStore],
		connectionListener: Option[ActorRef]
	): AmqpClientInterface =
		this.synchronized {
			if (config.testMode) {
				new NullAmqpClient
			} else {
				implicit val ex: ExecutionContext = ec
				val connection: ActorRef = createConnection(config, actorSystem)
				val bufferedMessageStores =
					messageStores.map {
						case (atLeastOnceGroup, messageStore) =>
							atLeastOnceGroup ->
								createMessageStore(config, actorSystem, logger, ec, messageStore)
					}

				val client = new AmqpClient(
					connection,
					actorSystem,
					config,
					logger,
					bufferedMessageStores,
					ec
				)
				connectionListener.foreach(client.addConnectionListener(_))
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

	private def createConnection(config: AmqpConfiguration, actorSystem: ActorSystem): ActorRef = {
		val factory = new ConnectionFactory()
		factory.setUsername(config.username)
		factory.setPassword(config.password)
		factory.setRequestedHeartbeat(config.heartbeatRate)
		factory.setConnectionTimeout(config.connectionTimeOut.toMillis.toInt)

		actorSystem.actorOf(
			ConnectionOwner.props(
				connFactory = factory,
				reconnectionDelay = config.connectionTimeOut,
				addresses = Some(config.addresses)
			)
		)
	}

	private def createMessageStore(
		config: AmqpConfiguration,
		actorSystem: ActorSystem,
		logger: Logger,
		ec: ExecutionContext,
		messageStore: MessageStore
	): MessageStore = {
		val resendLoopConfig: ResendLoopConfig = config.resendConfig.getOrElse(
			throw new IllegalStateException("No resendConfig for RabbitMQ exists")
		)

		new InMemoryMessageBufferDecorator(
			messageStore,
			actorSystem,
			logger,
			resendLoopConfig.memoryFlushInterval,
			resendLoopConfig.memoryFlushChunkSize,
			resendLoopConfig.memoryFlushTimeOut,
			config.askTimeOut
		)(ec)
	}

	/**
	 * Call disconnect on all created and tracked client.
	 */
	def disconnectAllClient(): Unit = clients.foreach(_.disconnect())

	/**
	 * Call reconnect on all created and tracked client.
	 */
	def reconnectAllClient(): Unit = clients.foreach(_.reconnect())

	/**
	 * Remove client from tracking.
	 * @param client client to remove from tracking
	 */
	def removeClient(client: AmqpConsumerClientInterface): Unit = this.synchronized {
		clients = clients - client
	}
}
