package com.kinja.amqp

import akka.Done
import akka.actor.{ ActorSystem, CoordinatedShutdown }
import akka.stream.Materializer
import akka.stream.scaladsl.{ Sink, Source }
import com.kinja.amqp.configuration.{ AmqpConfiguration, AtLeastOnceGroup }
import com.typesafe.config.{ Config, ConfigFactory }
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.util.{ Failure, Success }

object AmqpTestApp extends App {

	val configuration = new AmqpConfiguration {
		override protected lazy val config: Config = ConfigFactory.load("application-testApp")
	}

	print(configuration.exchanges.mkString(","))

	val factory = new AmqpClientFactory()
	val system = ActorSystem("test-producer-app")
	implicit val ec = scala.concurrent.ExecutionContext.global
	val materializer = Materializer(system)
	val logger = LoggerFactory.getLogger("test-producer-app")
	val inMemoryMessageStore = new InMemoryMessageStore

	private val coordinatedShutdown = CoordinatedShutdown(system)

	val client = factory.createClient(configuration, system, materializer, logger, ec, AtLeastOnceGroup.default -> inMemoryMessageStore)
	val producer = client.getMessageProducer("test-exchange")
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
		client.shutdown().map(_ => Done)
	}
}
