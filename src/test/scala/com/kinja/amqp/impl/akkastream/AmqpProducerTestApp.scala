package com.kinja.amqp.impl.akkastream

import akka.actor.{ ActorSystem, CoordinatedShutdown }
import akka.stream.Materializer
import akka.stream.alpakka.amqp.{ AmqpCachedConnectionProvider, AmqpConnectionFactoryConnectionProvider }
import akka.stream.scaladsl.{ Sink, Source }
import com.kinja.amqp.configuration.ExchangeParameters

import com.rabbitmq.client.ConnectionFactory
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.util.{ Failure, Success }

object AmqpProducerTestApp extends App {

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

	private val connectionProvider = AmqpCachedConnectionProvider( //Don't cache producer connection!
		AmqpConnectionFactoryConnectionProvider(
			factory,
		).withHostsAndPorts(Seq(("rabbit-gmginfra", 5672)))
	)
	private val logger = LoggerFactory.getLogger("consumer")

	private val producer: AtMostOnceAmqpProducer = new AtMostOnceAmqpProducer(
		connectionProvider = connectionProvider,
		logger = logger,
		materializer = materializer,
		params = ExchangeParameters(
			name = "test-exchange",
			passive = false,
			exchangeType = "topic",
			durable = true
		),
		system = system
	)

	import com.kinja.amqp.writesString
	val testStream = Source
		.fromIterator(() => (1 to 10000).iterator)
		.throttle(100, 1.seconds)
		.map(_.toString)
		.mapAsync(1) { msg =>
			producer.publish("test.binding", msg).recover {
				case ex =>
					logger.warn(s"$msg was not send because", ex)
			}
		}
		.runWith(Sink.ignore)(materializer)

	testStream.onComplete {
		case Success(result) => logger.info(s"TestStream completed with: $result")
		case Failure(exception) => logger.info(s"TestStream completed with failure", exception)
	}

	coordinatedShutdown.addTask(CoordinatedShutdown.PhaseServiceUnbind, "stop stream") { () =>
		producer.shutdown
	}
}
