package com.kinja.amqp.impl.akkastream

import akka.stream.alpakka.amqp.{ AmqpCachedConnectionProvider, AmqpConnectionFactoryConnectionProvider }
import com.github.sstone.amqp.Amqp.{ ExchangeParameters, QueueParameters }
import com.kinja.amqp.QueueWithRelatedParameters
import com.rabbitmq.client.ConnectionFactory
import com.typesafe.config.{ Config, ConfigFactory }

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

trait TestFactory {

	lazy val connectionFactory: ConnectionFactory = {
		val factory = new ConnectionFactory()
		factory.setUsername("guest")
		factory.setPassword("guest")
		factory.setRequestedHeartbeat(60)
		factory.setConnectionTimeout(100.millis.toMillis.toInt)
		factory.setAutomaticRecoveryEnabled(false)
		factory
	}

	lazy val config: Config = ConfigFactory.load()
	lazy val hosts: Seq[String] = config.getStringList("messageQueue.hosts").asScala.toSeq

	lazy val hostAndPorts: Seq[(String, Int)] = hosts.map((_, 5672))

	lazy val connectionProvider = createConnectionProvider(connectionFactory, hostAndPorts)

	lazy val defaultQueueWithRelatedParameters: QueueWithRelatedParameters = {
		QueueWithRelatedParameters(
			queueParams = QueueParameters(
				name = "test-queue",
				passive = false,
				durable = true,
				exclusive = false,
				autodelete = false,
				args = Map.empty[String, AnyRef]
			),
			boundExchange = ExchangeParameters(
				name = "test-exchange",
				passive = false,
				exchangeType = "topic",
				durable = true
			),
			bindingKey = "test.binding",
			deadLetterExchange = None
		)
	}

	private def createConnectionProvider(factory: ConnectionFactory, hostsAndPorts: Seq[(String, Int)]) = AmqpCachedConnectionProvider(
		AmqpConnectionFactoryConnectionProvider(
			factory,
		).withHostsAndPorts(hostsAndPorts)
	)

	def deleteBindingQueueAndExchange(queueParameters: QueueWithRelatedParameters): Unit = {
		val connection = connectionProvider.get
		val channel = connection.createChannel()
		channel.queueUnbind(queueParameters.queueParams.name, queueParameters.boundExchange.name, queueParameters.bindingKey)
		channel.queueDelete(queueParameters.queueParams.name)
		channel.exchangeDelete(queueParameters.boundExchange.name)
		channel.close()
		connectionProvider.release(connection)
	}
}
