package com.kinja.amqp

import com.kinja.amqp.persistence.{ MySqlMessageStore, InMemoryMessageBufferDecorator, MessageStore }

import akka.actor.{ ActorSystem, ActorRef }
import com.github.sstone.amqp.ConnectionOwner
import com.rabbitmq.client.ConnectionFactory
import javax.sql.DataSource
import org.slf4j.Logger
import scala.concurrent.ExecutionContext

class AmqpClientFactory {
	def createClient(
		config: AmqpConfiguration,
		actorSystem: ActorSystem,
		logger: Logger,
		ec: ExecutionContext,
		writeDataSource: DataSource,
		readDataSource: DataSource,
		hostname: String
	): AmqpClientInterface =
		{
			if (config.testMode) {
				new NullAmqpClient
			} else {
				val connection: ActorRef = createConnection(config, actorSystem)
				val messageStore: MessageStore = createMessageStore(
					config, actorSystem, logger, ec, writeDataSource, readDataSource, hostname
				)

				new AmqpClient(
					connection,
					actorSystem,
					config,
					logger,
					messageStore,
					ec
				)
			}
		}

	private def createConnection(config: AmqpConfiguration, actorSystem: ActorSystem): ActorRef = {
		val factory = new ConnectionFactory()
		factory.setUsername(config.username)
		factory.setPassword(config.password)
		factory.setRequestedHeartbeat(config.heartbeatRate)

		actorSystem.actorOf(
			ConnectionOwner.props(
				factory,
				config.connectionTimeOut,
				addresses = Some(config.addresses)
			)
		)
	}

	private def createMessageStore(
		config: AmqpConfiguration,
		actorSystem: ActorSystem,
		logger: Logger,
		ec: ExecutionContext,
		writeDataSource: DataSource,
		readDataSource: DataSource,
		hostname: String
	): MessageStore = {
		val resendLoopConfig: ResendLoopConfig = config.resendConfig.getOrElse(
			throw new IllegalStateException("No resendConfig for RabbitMQ exists")
		)

		new InMemoryMessageBufferDecorator(
			new MySqlMessageStore(
				hostname,
				writeDataSource,
				readDataSource
			),
			actorSystem,
			logger,
			resendLoopConfig.memoryFlushInterval,
			resendLoopConfig.memoryFlushChunkSize,
			resendLoopConfig.memoryFlushTimeOut,
			config.askTimeOut
		)(ec)
	}
}
