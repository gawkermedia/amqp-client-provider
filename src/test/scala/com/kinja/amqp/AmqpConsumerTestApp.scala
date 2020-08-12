package com.kinja.amqp

import akka.Done
import akka.actor.{ActorSystem, CoordinatedShutdown}
import akka.stream.Materializer
import com.kinja.amqp.configuration._
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random

object AmqpConsumerTestApp extends App {

	val configuration = new AmqpConfiguration {
		override protected lazy val config: Config = ConfigFactory.load("application-testApp")
	}

	print(configuration.exchanges.mkString(","))

	val factory = new AmqpClientFactory()
	val system = ActorSystem("test-consumer-app")
	implicit val ec = scala.concurrent.ExecutionContext.global
	val materializer = Materializer(system)
	val logger = LoggerFactory.getLogger("test-consumer-app")

	private val coordinatedShutdown = CoordinatedShutdown(system)

	val client = factory.createConsumerClient(configuration, system, materializer, logger, ec)
	val amqpConsumer = client.getMessageConsumer("test-app-queue")
	private val r = new Random(3L)
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

	coordinatedShutdown.addTask(CoordinatedShutdown.PhaseServiceUnbind, "stop consumer stream") { () =>
		client.shutdown().map(_ => Done)
	}
}
