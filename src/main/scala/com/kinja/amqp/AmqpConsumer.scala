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

import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

class AmqpConsumer(
	connection: ActorRef,
	actorSystem: ActorSystem,
	connectionTimeOut: FiniteDuration,
	logger: Slf4jLogger
)(val params: QueueWithRelatedParameters) extends AmqpConsumerInterface {

	def subscribe[A: Reads](processor: A => Unit): Unit = {
		val listener = createListener(processor)

		val initDeadLetterExchangeRequest = params.deadLetterExchange.map(
			exchangeParams => Record(DeclareExchange(exchangeParams))
		)
		// we don't have to declare bound exchange and the queue itself, because it's done with the AddBinding
		val bindingRequest = Some(
			Record(AddBinding(Binding(params.boundExchange, params.queueParams, params.bindingKey)))
		)
		val initRequests = List(initDeadLetterExchangeRequest, bindingRequest).flatten

		val consumer = ConnectionOwner.createChildActor(
			connection,
			Consumer.props(listener = Some(listener), init = initRequests, autoack = false),
			Some("consumer_" + params.queueParams.name)
		)

		Amqp.waitForConnection(actorSystem, connection, consumer).await(connectionTimeOut.toSeconds, TimeUnit.SECONDS)
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
								logger.warn(s"""[RabbitMQ] Couldn't parse json "$json" : $e""")
								sender ! Reject(envelope.getDeliveryTag, requeue = false)
							case NonFatal(t) =>
								logger.warn(s"""[RabbitMQ] Exception while processing message "$json" : $t""")
								sender ! Reject(envelope.getDeliveryTag, requeue = true)
						}
					} catch {
						case NonFatal(t) =>
							logger.warn(s"""[RabbitMQ] Couldn't parse string "$s" as json: $t""")
							sender ! Reject(envelope.getDeliveryTag, requeue = false)
					}
			}
		}))
	}
}