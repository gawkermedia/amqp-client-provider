package com.kinja.amqp

import akka.actor._
import com.github.sstone.amqp.Amqp._
import com.rabbitmq.client.AMQP.BasicProperties

import scala.concurrent.{ ExecutionContext, Future }

class AtMostOnceAmqpProducer(
	exchangeName: String,
	channelProvider: ProducerChannelProvider
)(implicit val ec: ExecutionContext) extends AmqpProducerInterface {

	private val channel: ActorRef = channelProvider.createChannel()

	def publish[A: Writes](
		routingKey: String,
		message: A,
		saveTimeMillis: Long = System.currentTimeMillis()
	): Future[Unit] = {
		val messageString = implicitly[Writes[A]].writes(message)
		val bytes = messageString.getBytes(java.nio.charset.Charset.forName("UTF-8"))
		val properties = new BasicProperties.Builder().deliveryMode(2).build()
		channel ! Publish(
			exchangeName, routingKey, bytes, properties = Some(properties), mandatory = true, immediate = false
		)
		Future.successful(())
	}
}
