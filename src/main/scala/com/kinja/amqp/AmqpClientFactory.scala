package com.kinja.amqp

import com.kinja.amqp.persistence.{ InMemoryMessageBufferDecorator, MessageStore }
import akka.actor.{ ActorRef, ActorSystem }
import com.github.sstone.amqp.ConnectionOwner
import com.rabbitmq.client.ConnectionFactory
import utils._

import org.slf4j.Logger

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.control.NonFatal

class AmqpClientFactory {

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
		messageStores: List[(AtLeastOnceGroup, MessageStore)],
		connectionListener: Option[ActorRef]
	): AmqpClientInterface =
		createClient(config, actorSystem, logger, ec, messageStores.toMap, connectionListener)

	def createClient(
		config: AmqpConfiguration,
		actorSystem: ActorSystem,
		logger: Logger,
		ec: ExecutionContext,
		messageStores: Map[AtLeastOnceGroup, MessageStore],
		connectionListener: Option[ActorRef]
	): AmqpClientInterface =
		{
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
}
