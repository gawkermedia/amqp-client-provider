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

	val actorName = "consumer_" + params.queueParams.name

	/**
	 * @inheritdoc
	 */
	override def disconnect(): Unit = {
		actorSystem.actorSelection(actorSystem / actorName) ! Disconnect
	}

	/**
	 * @inheritdoc
	 */
	override def reconnect(): Unit = {
		actorSystem.actorSelection(actorSystem / actorName) ! Connect
	}

	/**
	 * @inheritdoc
	 */
	override def subscribe[A: Reads](timeout: FiniteDuration)(processor: A => Future[Unit]): Unit =
		subscribe(timeout, Duration.Zero, processor)

	/**
	 * @inheritdoc
	 */
	override def subscribe[A: Reads](timeout: FiniteDuration, spacing: FiniteDuration, processor: A => Future[Unit]): Unit = {
		val rejecter = actorSystem.actorOf(Props(new Rejecter))

		val proxy = actorSystem.actorOf(Props(new Proxy))
		proxy ! SetListener(rejecter)

		val supervisor = actorSystem.actorOf(
			Props(new Supervisor(timeout, spacing, processor, rejecter, proxy, logger)),
			actorName)
		supervisor ! Connect

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
			Consumer.props(
				listener = Some(proxy),
				channelParams = channelParams,
				init = initRequests,
				autoack = false)
		)

		ignore(Amqp.waitForConnection(actorSystem, connection, consumer).await(connectionTimeOut.toSeconds, TimeUnit.SECONDS))
	}
}

class Listener[A: Reads](
	timeout: FiniteDuration,
	spacing: FiniteDuration,
	processor: A => Future[Unit],
	logger: Slf4jLogger
) extends Actor {

	private case object WakeUp

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
		case WakeUp => ()
	}

	def asleep(originalSender: ActorRef, ack: Ack): Receive = {
		case WakeUp =>
			originalSender ! ack
			context.unbecome()
		case Delivery(_, envelope, _, _) =>
			sender ! Reject(envelope.getDeliveryTag, requeue = true)
	}
}

class Rejecter extends Actor {
	def receive = {
		case Delivery(_, envelope, _, _) =>
			sender ! Reject(envelope.getDeliveryTag, requeue = true)
	}
}

final case class SetListener(listener: ActorRef)

class Proxy extends Actor {
	def receive = {
		case SetListener(listener) => context.become(forwarding(listener))
	}
	def forwarding(listener: ActorRef): Receive = {
		case SetListener(otherListener) => context.become(forwarding(otherListener))
		case message => listener.forward(message)
	}
}

case object Connect
case object Disconnect

class Supervisor[A: Reads](
	timeout: FiniteDuration,
	spacing: FiniteDuration,
	processor: A => Future[Unit],
	rejecter: ActorRef,
	proxy: ActorRef,
	logger: Slf4jLogger) extends Actor {

	def receive = {
		case Disconnect => ()
		case Connect =>
			val listener = context.actorOf(Props(new Listener(timeout, spacing, processor, logger)))
			proxy ! SetListener(listener)
			context.become(connected(listener))

	}

	def connected(listener: ActorRef): Receive = {
		case Connect => ()
		case Disconnect =>
			context.stop(listener) // Let listener handle one more message
			proxy ! SetListener(rejecter) // Some messages would be lost, and requeued later
			context.unbecome()
	}

}
