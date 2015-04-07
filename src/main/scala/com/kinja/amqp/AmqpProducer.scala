package com.kinja.amqp

import java.util.UUID

import com.kinja.amqp.model.MessageConfirmation
import com.kinja.amqp.model.Message

import com.github.sstone.amqp.ChannelOwner.NotConnectedError
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

import java.util.concurrent.TimeUnit

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Try

class AmqpProducer(
	connection: ActorRef,
	actorSystem: ActorSystem,
	messageStore: MessageStore,
	connectionTimeOut: Long,
	logger: Slf4jLogger)(exchange: ExchangeParameters) {

	private val channel: ActorRef = createChannel()

	Amqp.waitForConnection(actorSystem, connection, channel).await(connectionTimeOut, TimeUnit.SECONDS)

	channel ! ConfirmSelect
	channel ! AddConfirmListener(createConfirmListener)

	def publish[A: Writes](routingKey: String, message: A, saveTimeMillis: Long = System.currentTimeMillis()): Future[Try[Unit]] = {
		val json = Json.toJson(message)
		val bytes = json.toString.getBytes(java.nio.charset.Charset.forName("UTF-8"))
		val properties = new BasicProperties.Builder().deliveryMode(2).build()
		//TODO: this should come from config
		implicit val timeout = Timeout(5.seconds)
		//TODO: exec context
		import scala.concurrent.ExecutionContext.Implicits.global
		channel ? Publish(exchange.name, routingKey, bytes, properties = Some(properties), mandatory = true, immediate = false) map { resp =>
			resp match {
				case ok: Ok => {
					val data = ok.result.get.asInstanceOf[(Long, UUID)]
					//TODO: use new key class
					messageStore.saveMessage(Message(None, routingKey, json.toString, Some(data._2), Some(data._1), saveTimeMillis))
				}
				case err: NotConnectedError => messageStore.saveMessage(Message(None, routingKey, json.toString, None, None, saveTimeMillis))
				case _ => messageStore.saveMessage(Message(None, routingKey, json.toString, None, None, saveTimeMillis))
			}
		} recoverWith {
			case _ => Future(messageStore.saveMessage(Message(None, routingKey, json.toString, None, None, saveTimeMillis)))
		}
	}

	private def createChannel(): ActorRef = {
		ConnectionOwner.createChildActor(connection, ChannelOwner.props(init = List(Record(DeclareExchange(exchange)))))
	}

	private def createConfirmListener: ActorRef = actorSystem.actorOf(Props(new Actor {
		def receive = {
			case HandleAck(deliveryTag, multiple) =>
				//TODO: publish local
				val channelId = UUID.randomUUID()
				messageStore.saveConfirmation(MessageConfirmation(None, channelId, deliveryTag, multiple))
			case HandleNack(deliveryTag, multiple) =>
			//TODO: log
		}
	}))
}