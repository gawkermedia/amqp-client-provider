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

import java.util.concurrent.TimeUnit

import scala.concurrent.{ Future, ExecutionContext, Await }
import scala.concurrent.duration._
import scala.util.control.NonFatal

class AmqpConsumer(
	connection: ActorRef,
	actorSystem: ActorSystem,
	connectionTimeOut: FiniteDuration,
	logger: Slf4jLogger
)(val params: QueueWithRelatedParameters) extends AmqpConsumerInterface {

	/**
	 * @inheritdoc
	 */
	override def subscribe[A: Reads](timeout: FiniteDuration)(processor: A => Future[Unit]): Unit =
		subscribe(timeout, Duration.Zero, processor)

	/**
	 * @inheritdoc
	 */
	override def subscribe[A: Reads](timeout: FiniteDuration, spacing: FiniteDuration, processor: A => Future[Unit]): Unit = {
		val listener = createListener(timeout, spacing, processor)

		val initDeadLetterExchangeRequest = params.deadLetterExchange.map(
			exchangeParams => Record(DeclareExchange(exchangeParams))
		)
		// we don't have to declare bound exchange and the queue itself, because it's done with the AddBinding
		val bindingRequest = Some(
			Record(AddBinding(Binding(params.boundExchange, params.queueParams, params.bindingKey)))
		)
		val initRequests = List(initDeadLetterExchangeRequest, bindingRequest).flatten

		// make sure to only consume one message at a time of rate limiting is enabled
		val channelParams = if (spacing.toNanos > 0) Some(ChannelParameters(1)) else None

		val consumer = ConnectionOwner.createChildActor(
			connection,
			Consumer.props(listener = Some(listener), channelParams = channelParams, init = initRequests, autoack = false),
			Some("consumer_" + params.queueParams.name)
		)

		ignore(Amqp.waitForConnection(actorSystem, connection, consumer).await(connectionTimeOut.toSeconds, TimeUnit.SECONDS))
	}

	private case object WakeUp

	private def createListener[A: Reads](timeout: FiniteDuration, spacing: FiniteDuration, processor: A => Future[Unit]): ActorRef = {
		actorSystem.actorOf(Props(new Actor {

			/**
			 * Processing of next message may start immediately if the time until next tick is less
			 * than this number.
			 */
			private val toleranceNanos = 10000000L // 10 milliseconds

			/**
			 * Default state of the listener which receives messages from RabbitMQ.
			 */
			def receive = {
				case Delivery(consumerTag, envelope, properties, body) =>
					val nextTickNanos = System.nanoTime + spacing.toNanos
					val s = new String(body, "UTF-8")

					implicitly[Reads[A]].reads(s) match {
						case Right(message) =>
							try {
								Await.result(processor(message), timeout)
								val ack = Ack(envelope.getDeliveryTag)

								// sleep until we are allowd to receive a new message
								val nowNanos = System.nanoTime
								if (nowNanos < nextTickNanos - toleranceNanos) {
									implicit val ec: ExecutionContext = context.dispatcher
									context.system.scheduler.scheduleOnce((nextTickNanos - nowNanos).nanos, self, WakeUp)
									context.become(asleep(sender, ack))
								} else {
									sender ! ack
								}
							} catch {
								case NonFatal(t) =>
									logger.warn(s"""[RabbitMQ] Exception while processing message "$s" : $t""")
									sender ! Reject(envelope.getDeliveryTag, requeue = true)
							}
						case Left(e) =>
							logger.warn(s"""[RabbitMQ] Couldn't parse message "$s" : $e""")
							sender ! Reject(envelope.getDeliveryTag, requeue = false)
					}
			}

			def asleep(originalSender: ActorRef, ack: Ack): Receive = {
				case WakeUp =>
					originalSender ! ack
					context.unbecome()
				case Delivery(_, envelope, _, _) =>
					sender ! Reject(envelope.getDeliveryTag, requeue = true)
			}
		}))
	}
}
