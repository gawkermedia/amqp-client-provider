package com.kinja.amqp

import com.github.sstone.amqp.Amqp._
import com.github.sstone.amqp.ConnectionOwner
import com.github.sstone.amqp.Consumer
import com.github.sstone.amqp.Amqp

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.Actor

import org.slf4j.{ Logger => Slf4jLogger }

import play.api.libs.json._

import java.util.concurrent.TimeUnit

import scala.util.control.NonFatal

class AmqpConsumer(
	connection: ActorRef,
	actorSystem: ActorSystem,
	connectionTimeOut: Long,
	logger: Slf4jLogger)(params: QueueWithRelatedParameters) {

	def subscribe[A: Reads](processor: A => Unit): Unit = {
		val listener = createListener(processor)

		val initDeadLetterExchangeRequest = params.deadLetterExchange.map(DeclareExchange)
		// we don't have to declare bound exchange and the queue itself, because it's done with the AddBinding
		val bindingRequest = Some(AddBinding(Binding(params.boundExchange, params.queueParams, params.bindingKey)))
		val initReqests = List(initDeadLetterExchangeRequest, bindingRequest).flatten

		val consumer = ConnectionOwner.createChildActor(
			connection,
			Consumer.props(
				listener = Some(listener),
				init = initReqests,
				autoack = false),
			name = Some("consumer_" + params.queueParams.name))

		Amqp.waitForConnection(actorSystem, connection, consumer).await(connectionTimeOut, TimeUnit.SECONDS)
	}

	private def createListener[A: Reads](processor: A => Unit): ActorRef = {
		actorSystem.actorOf(Props(new Actor {
			def receive = {
				case Delivery(consumerTag, envelope, properties, body) =>
					val s = new String(body, "UTF-8")
					try {
						val json = Json.parse(s)
						try {
							val message = json.as[A]
							processor(message)
							sender ! Ack(envelope.getDeliveryTag)
						} catch {
							case e: JsResultException =>
								logger.warn(s"""Couldn't parse json "$json" : $e""")
								sender ! Reject(envelope.getDeliveryTag, requeue = false)
							case NonFatal(t) =>
								logger.warn(s"""Exception while processing message "$json" : $t""")
								sender ! Reject(envelope.getDeliveryTag, requeue = true)
						}
					} catch {
						case NonFatal(t) =>
							logger.warn(s"""Couldn't parse string "$s" as json: $t""")
							sender ! Reject(envelope.getDeliveryTag, requeue = false)
					}

			}
		}))
	}
}