package com.kinja.amqp.impl.akkastream

import java.sql.Timestamp
import java.util.UUID

import akka.Done
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.alpakka.amqp.AmqpConnectionProvider

import com.kinja.amqp.configuration.ExchangeParameters
import com.kinja.amqp.model.FailedMessage
import com.kinja.amqp.persistence.MessageStore
import com.kinja.amqp.{ AmqpProducerInterface, WithShutdown, Writes }
import org.slf4j.Logger

import scala.concurrent.Future
import scala.util.control.NonFatal

class AtLeastOnceAmqpProducer(
	connectionProvider: AmqpConnectionProvider,
	logger: Logger,
	system: ActorSystem,
	materializer: Materializer,
	params: ExchangeParameters,
	messageStore: MessageStore) extends AmqpProducerInterface with WithShutdown {

	private implicit val ec = system.dispatcher

	private val producer = new AtMostOnceAmqpProducer(
		connectionProvider = connectionProvider,
		logger = logger,
		system = system,
		materializer = materializer,
		params = params
	)

	override def publish[A: Writes](
		routingKey: String,
		message: A,
		saveTimeMillis: Long = System.currentTimeMillis()): Future[Unit] = {
		publish(routingKey, message, UUID.randomUUID(), saveTimeMillis)
	}

	override def publish[A: Writes](
		routingKey: String,
		message: A,
		messageId: UUID,
		saveTimeMillis: Long): Future[Unit] = {

		producer
			.publish(routingKey, message, messageId, saveTimeMillis)
			.recoverWith {
				case NonFatal(e) =>
					logger.warn(s"[RabbitMQ] Message sent failed, storing message(messageId= ${messageId.toString()}) in message store.", e)
					messageStore.saveFailedMessages(List(FailedMessage(
						id = None,
						routingKey = routingKey,
						exchangeName = params.name,
						message = implicitly[Writes[A]].writes(message),
						messageId = messageId,
						createdTime = new Timestamp(saveTimeMillis)
					)))
			}
	}

	override def shutdown: Future[Done] = {
		for {
			_ <- producer.shutdown
			_ <- messageStore.shutdown()
		} yield Done
	}
}
