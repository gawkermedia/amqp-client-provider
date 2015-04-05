package com.kinja.amqp

import com.github.sstone.amqp.ConnectionOwner
import com.github.sstone.amqp.ChannelOwner
import com.github.sstone.amqp.Amqp
import com.github.sstone.amqp.Amqp._

import com.rabbitmq.client.AMQP.BasicProperties

import akka.actor.ActorRef
import akka.actor.ActorSystem

import org.slf4j.{ Logger => Slf4jLogger }

import play.api.libs.json._

import java.util.concurrent.TimeUnit

class AmqpProducer(
	connection: ActorRef,
	actorSystem: ActorSystem,
	connectionTimeOut: Long,
	logger: Slf4jLogger)(exchange: ExchangeParameters) {

	private val channel: ActorRef = createChannel()

	Amqp.waitForConnection(actorSystem, connection, channel).await(connectionTimeOut, TimeUnit.SECONDS)

	def publish[A: Writes](routingKey: String, message: A): Unit = {
		val json = Json.toJson(message)
		val bytes = json.toString.getBytes(java.nio.charset.Charset.forName("UTF-8"))
		val properties = new BasicProperties.Builder().deliveryMode(2).build()
		channel ! Publish(exchange.name, routingKey, bytes, properties = Some(properties), mandatory = true, immediate = false)
	}

	private def createChannel(): ActorRef = {
		ConnectionOwner.createChildActor(connection, ChannelOwner.props(init = List(Record(DeclareExchange(exchange)))))
	}
}