package com.kinja.amqp

import com.kinja.amqp.model.MessageConfirmation
import com.kinja.amqp.model.Message

import com.github.sstone.amqp.ConnectionOwner
import com.github.sstone.amqp.ChannelOwner
import com.github.sstone.amqp.Amqp
import com.github.sstone.amqp.Amqp._

import com.rabbitmq.client.AMQP.BasicProperties

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.pattern.ask
import akka.util.Timeout

import org.slf4j.{ Logger => Slf4jLogger }

import play.api.libs.json._

import java.util.Date
import java.util.concurrent.TimeUnit

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._

class AmqpProducer(
	connection: ActorRef,
	actorSystem: ActorSystem,
	messageStore: MessageStore,
	connectionTimeOut: Long,
	askTimeout: Long,
	logger: Slf4jLogger
)(exchange: ExchangeParameters) {

	private implicit val timeout = Timeout(askTimeout.seconds)
	private val channel: ActorRef = createChannel()

	def publish[A: Writes](
		routingKey: String, message: A, saveTimeMillis: Long = System.currentTimeMillis()
	)(implicit ec: ExecutionContext): Future[Unit] = {
		val json = Json.toJson(message)
		val bytes = json.toString.getBytes(java.nio.charset.Charset.forName("UTF-8"))
		val properties = new BasicProperties.Builder().deliveryMode(2).build()
		channel ? Publish(exchange.name, routingKey, bytes, properties = Some(properties), mandatory = true, immediate = false) map {
			case Ok(_, Some(MessageUniqueKey(deliveryTag, channelId))) => {
				messageStore.saveMessage(
					Message(None, routingKey, exchange.name, json.toString, Some(channelId), Some(deliveryTag), new Date(saveTimeMillis))
				)
			}
			case _ => messageStore.saveMessage(Message(None, routingKey, exchange.name, json.toString, None, None, new Date(saveTimeMillis)))
		} recoverWith {
			case _ => Future.successful(
				messageStore.saveMessage(Message(None, routingKey, exchange.name, json.toString, None, None, new Date(saveTimeMillis)))
			)
		}
	}

	private def createChannel(): ActorRef = {
		val channel = ConnectionOwner.createChildActor(
			connection, ChannelOwner.props(init = List(Record(DeclareExchange(exchange))))
		)

		Amqp.waitForConnection(actorSystem, connection, channel).await(connectionTimeOut, TimeUnit.SECONDS)

		channel ! ConfirmSelect
		channel ! AddConfirmListener(createConfirmListener)

		channel
	}

	private def handleConfirm(
		channelId: String, deliveryTag: Long, multiple: Boolean, timestamp: Long
	): Unit = {
		if (multiple)
			messageStore.saveConfirmation(MessageConfirmation(None, channelId, deliveryTag, multiple, new Date(timestamp)))
		else {
			if (messageStore.deleteMessageUponConfirm(channelId, deliveryTag) > 0) {}
			else
				messageStore.saveConfirmation(MessageConfirmation(None, channelId, deliveryTag, multiple, new Date(timestamp)))
		}
	}

	private def createConfirmListener: ActorRef = actorSystem.actorOf(Props(new Actor {
		def receive = {
			case HandleAck(deliveryTag, multiple, channelId, timestamp) =>
				handleConfirm(channelId, deliveryTag, multiple, timestamp)
			case HandleNack(deliveryTag, multiple, channelId, timestamp) =>
				logger.warn(
					s"""Receiving HandleNack with delivery tag: $deliveryTag,
					 | multiple: $multiple, channelId: $channelId"""
				)
		}
	}))
}