package com.kinja.amqp

import com.kinja.amqp.persistence.{ InMemoryMessageBufferDecorator, MessageStore }

import akka.actor.{ ActorSystem, ActorRef }
import com.github.sstone.amqp.ConnectionOwner
import com.rabbitmq.client.ConnectionFactory
import java.sql.Connection
import org.slf4j.Logger
import scala.concurrent.ExecutionContext

class AmqpClientFactory {

	def createConsumerClient(
		config: AmqpConfiguration,
		actorSystem: ActorSystem,
		logger: Logger,
		ec: ExecutionContext
	): AmqpConsumerClientInterface =
		createClient(config, actorSystem, logger, ec, Map.empty[AtLeastOnceGroup, MessageStore])

	def createClient(
		config: AmqpConfiguration,
		actorSystem: ActorSystem,
		logger: Logger,
		ec: ExecutionContext,
		messageStores: Map[AtLeastOnceGroup, MessageStore]
	): AmqpClientInterface =
		{
			if (config.testMode) {
				new NullAmqpClient
			} else {
				val connection: ActorRef = createConnection(config, actorSystem)
				val bufferedMessageStores =
					messageStores.map {
						case (atLeastOnceGroup, messageStore) =>
							atLeastOnceGroup ->
								createMessageStore(config, actorSystem, logger, ec, messageStore)
					}

				new AmqpClient(
					connection,
					actorSystem,
					config,
					logger,
					bufferedMessageStores,
					ec
				)
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
