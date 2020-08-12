package com.kinja.amqp.impl.akkastream

import akka.actor.{ ActorSystem, CoordinatedShutdown }
import akka.stream.Materializer
import akka.stream.alpakka.amqp.{ AmqpCachedConnectionProvider, AmqpConnectionFactoryConnectionProvider }
import com.kinja.amqp.configuration._
import com.rabbitmq.client.ConnectionFactory
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random

object AmqpConsumerTestApp extends App {

	private val factory = new ConnectionFactory()
	factory.setUsername("guest")
	factory.setPassword("guest")
	factory.setRequestedHeartbeat(60)
	factory.setConnectionTimeout(1000.millis.toMillis.toInt)
	factory.setAutomaticRecoveryEnabled(false)
	factory.setTopologyRecoveryEnabled(false)

	private val system = ActorSystem("test")
	import scala.concurrent.ExecutionContext.Implicits.global
	private val materializer = Materializer(system)

	private val coordinatedShutdown = CoordinatedShutdown(system)

	private val connectionProvider = AmqpCachedConnectionProvider(
		AmqpConnectionFactoryConnectionProvider(
			factory,
		).withHostsAndPorts(Seq(("localhost", 5672)))
	)
	private val logger = LoggerFactory.getLogger("consumer")
	private val consumerConfig = ConsumerConfig(
		connectionTimeOut = 1.second,
		shutdownTimeout = 5.seconds,
		defaultPrefetchCount = 10,
		reconnectionTime = 5.seconds,
		defaultParallelism = 4,
		defaultThrottling = Throttling(100, 1.seconds),
		defaultProcessingTimeout = 5.seconds
	)
	private val amqpConsumer: AmqpConsumer = new AmqpConsumer(
		connectionProvider = connectionProvider,
		logger = logger,
		system = system,
		materializer = materializer,
		consumerConfig = consumerConfig
	)(params = QueueWithRelatedParameters(
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
	))

	private val r = new Random(3L)
	import com.kinja.amqp.readsString
	amqpConsumer.subscribe[String](5.seconds)(s => Future {

		if ("timeout".equalsIgnoreCase(s) && r.nextBoolean()) {
			logger.info("timeout message received. Timeouting.")
			Thread.sleep(10000)
		} else if ("failure".equalsIgnoreCase(s) && r.nextBoolean()) {
			logger.info("failure message received. Failing.")
			throw new Exception("Failure message received!")
		} else {
			logger.info(s"Message processed: $s")
		}
	})

	coordinatedShutdown.addTask(CoordinatedShutdown.PhaseServiceUnbind, "stop stream") { () =>
		amqpConsumer.shutdown
	}
}
