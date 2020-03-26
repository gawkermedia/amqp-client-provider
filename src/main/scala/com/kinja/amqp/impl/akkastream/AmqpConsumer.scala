package com.kinja.amqp.impl.akkastream

import java.io.IOException

import akka.Done
import akka.pattern.pipe
import akka.actor.{Actor, ActorRef, ActorSystem, Props, Stash, Status, Timers}
import akka.stream.alpakka.amqp.scaladsl.AmqpSource
import akka.stream.alpakka.amqp.{AmqpConnectionProvider, BindingDeclaration, ExchangeDeclaration, NamedQueueSourceSettings, QueueDeclaration}
import akka.stream.scaladsl.Sink
import akka.stream.{KillSwitches, Materializer, SharedKillSwitch, ThrottleMode}
import com.kinja.amqp.impl.akkastream.Subscription.{CheckBinding, Connect, Disconnect}
import com.kinja.amqp.{AmqpConsumerInterface, QueueWithRelatedParameters, Reads}
import com.kinja.amqp.ignore
import org.slf4j.{Logger => Slf4jLogger}

import scala.jdk.CollectionConverters._
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class AmqpConsumer(
	connectionProvider: AmqpConnectionProvider,
	logger: Slf4jLogger,
	system: ActorSystem,
	materializer: Materializer,
	processorEx: ExecutionContext,
	connectionTimeOut: FiniteDuration,
	defaultPrefetchCount: Int)(
	params: QueueWithRelatedParameters) extends AmqpConsumerInterface {

	@SuppressWarnings(Array("org.wartremover.warts.Var"))
	private var subscription: Option[ActorRef] = None
	/**
	 * Stops processing messages from now on, effectively unsubscribing from the queue.
	 * If no subscription was performed before, it does nothing.
	 */
	override def disconnect(): Unit = {
		subscription.foreach(_ ! Disconnect)
	}

	/**
	 * Resumes processing messages from now on, as if resubscribed to the queue.
	 * If no subscription was performed before, it does nothing.
	 */
	override def reconnect(): Unit = {
		subscription.foreach(_ ! Connect)
	}

	/**
	 * Subscribes the message processor function to consume the queue described by params.
	 *
	 * @param timeout   The maximum amount of time to wait for processing to complete.
	 * @param processor The pmessage processor function.
	 */
	override def subscribe[A: Reads](timeout: FiniteDuration)(processor: A => Future[Unit]): Unit = {
		val config = SubscriptionConfig[A](
			timeout = timeout,
			processor = processor
		)
		createSubscription(config)
	}

	/**
	 * Subscribes the message processor function to consume the queue described by params.
	 *
	 * @param timeout       The maximum amount of time to wait for processing to complete.
	 * @param prefetchCount The maximum amount of "in flight" messages.
	 *                      If not set to None the prefetch count will be unlimited.
	 *                      Default value set to 10.
	 *                      (https://www.rabbitmq.com/amqp-0-9-1-reference.html#basic.qos.prefetch-count)
	 * @param processor     The message processor function.
	 */
	override def subscribe[A: Reads](timeout: FiniteDuration, prefetchCount: Option[Int])(processor: A => Future[Unit]): Unit = {
		val config = SubscriptionConfig[A](
			timeout = timeout,
			prefetchCount = prefetchCount,
			processor = processor
		)
		createSubscription(config)
	}

	/**
	 * Subscribes the message processor function to consume the queue described by params.
	 *
	 * @param timeout   The maximum amount of time to wait for processing to complete.
	 * @param spacing   The minimum amount of time that has to elapse between starting processing
	 *                  new messages. It can be used to define rate limiting, for example, setting 10
	 *                  seconds here means that only one message may be processed each 10 seconds, resulting
	 *                  in a processing rate of 0.1 message/sec. Note that this is not the time to wait
	 *                  between processing messages (end of last and beginning of next) but rather
	 *                  the time between the createStream of each processing. This means, sticking to the previous
	 *                  example, that if processing took more than 10 seconds, processing the next message
	 *                  can immediately be started.
	 * @param processor The pmessage processor function.
	 */
	override def subscribe[A: Reads](timeout: FiniteDuration, spacing: FiniteDuration, processor: A => Future[Unit]): Unit = {
		val config = SubscriptionConfig[A](
			timeout = timeout,
			spacing = Some(spacing),
			processor = processor
		)
		createSubscription(config)
	}

	/**
	 * Subscribes the message processor function to consume the queue described by params.
	 *
	 * @param timeout       The maximum amount of time to wait for processing to complete.
	 * @param prefetchCount The maximum amount of "in flight" messages.
	 *                      If not set the prefetch count will be unlimited.
	 *                      If spacing set greater than zero this value overridden with 1.
	 *                      Default value is 10.
	 *                      (https://www.rabbitmq.com/amqp-0-9-1-reference.html#basic.qos.prefetch-count)
	 * @param spacing       The minimum amount of time that has to elapse between starting processing
	 *                      new messages. It can be used to define rate limiting, for example, setting 10
	 *                      seconds here means that only one message may be processed each 10 seconds, resulting
	 *                      in a processing rate of 0.1 message/sec. Note that this is not the time to wait
	 *                      between processing messages (end of last and beginning of next) but rather
	 *                      the time between the createStream of each processing. This means, sticking to the previous
	 *                      example, that if processing took more than 10 seconds, processing the next message
	 *                      can immediately be started.
	 * @param processor     The pmessage processor function.
	 */
	override def subscribe[A: Reads](timeout: FiniteDuration, prefetchCount: Option[Int], spacing: FiniteDuration, processor: A => Future[Unit]): Unit = {
		val config = SubscriptionConfig[A](
			timeout = timeout,
			prefetchCount = prefetchCount,
			spacing = Some(spacing),
			processor = processor
		)
		createSubscription(config)
	}

	private def createSubscription[A: Reads](subscriptionConfig: SubscriptionConfig[A]): Unit = {
		subscription = subscription match {
			case s @ Some(_) =>
				logger.warn(s"MessageConsumer for ${params.queueParams.name} already subscribed!")
				s
			case None =>
				implicit val procEx: ExecutionContext = processorEx
				implicit val mat: Materializer = materializer
				val subscription = system.actorOf(
					props = Subscription.props[A](
						config = subscriptionConfig,
						settings = baseSettings,
						defaultPrefetchCount = defaultPrefetchCount,
						reconnectTime = 1.seconds,
						logger = logger
					),
					name = s"ampqp_consumer_${baseSettings.queue}"
				)
				subscription ! Connect
				Some(subscription)
		}
	}

	private lazy val baseSettings: NamedQueueSourceSettings = {
		NamedQueueSourceSettings(connectionProvider, params.queueParams.name).withDeclarations(
			Seq(
				ExchangeDeclaration(params.boundExchange.name, params.boundExchange.exchangeType)
					.withDurable(params.boundExchange.durable)
					.withAutoDelete(params.boundExchange.autodelete)
					.withArguments(params.boundExchange.args),
				BindingDeclaration(params.queueParams.name, params.boundExchange.name)
					.withRoutingKey(params.bindingKey),
				QueueDeclaration(params.queueParams.name)
					.withDurable(params.queueParams.durable)
					.withAutoDelete(params.queueParams.autodelete)
					.withExclusive(params.queueParams.exclusive)
			)
		)
	}
}

final class Subscription[A: Reads](
	config: SubscriptionConfig[A],
	materializer: Materializer,
	processorEx: ExecutionContext,
	settings: NamedQueueSourceSettings,
	defaultPrefetchCount: Int,
	reconnectTime: FiniteDuration,
	logger: Slf4jLogger) extends Actor with Stash with Timers {

	import context.dispatcher
	private val superVisorName = s"ampqp_consumer_${settings.queue}"

	override def preStart(): Unit = {
		createExchangeQueueAndBindingIfNotExists()
		timers.startTimerWithFixedDelay("check-queue", CheckBinding, 5.seconds)
		super.preStart()
	}

	override def receive: Receive = disconnected

	private def disconnected: Receive = {
		case Connect =>
			val killSwitch = KillSwitches.shared(superVisorName)
			val streamDone = createStream(killSwitch)
			ignore(streamDone pipeTo self)
			context.become(connected(killSwitch, streamDone))
		case CheckBinding =>
			createExchangeQueueAndBindingIfNotExists()
	}

	@SuppressWarnings(Array("org.wartremover.warts.IsInstanceOf"))
	private def connected(killSwitch: SharedKillSwitch, streamDone: Future[Done]): Receive = {
		case Disconnect =>
			killSwitch.shutdown()
			ignore(streamDone pipeTo sender())
			context.become(disconnected)
		case Status.Failure(ex) =>
			logger.error(s"Stream failed with: ${ex.getClass.getName}: ${ex.getMessage}. Reconnecting!")
			timers.startSingleTimer(s"reconnecting_$superVisorName", Connect, reconnectTime)
			context.become(disconnected)
		case Status.Success(_) =>
			logger.warn(s"Stream ${superVisorName} finished!")
			context.become(disconnected)
		case CheckBinding =>
			createExchangeQueueAndBindingIfNotExists()
	}

	private def createExchangeQueueAndBindingIfNotExists() = {
		val connection = settings.connectionProvider.get
		if (connection.isOpen) {
			val channel = connection.createChannel()
			ignore(settings.declarations.collectFirst {
				case e: ExchangeDeclaration =>
					logger.debug("Declaring exchange")
					channel.exchangeDeclare(e.name, e.exchangeType, e.durable, e.autoDelete, e.arguments.asJava)
			})
			ignore(settings.declarations.collectFirst {
				case qd: QueueDeclaration =>
					logger.debug("Declaring queue")
					channel.queueDeclare(qd.name, qd.durable, qd.exclusive, qd.autoDelete, qd.arguments.asJava)
			})
			ignore(settings.declarations.collectFirst {
				case binding: BindingDeclaration =>
					logger.debug("Declare binding")
					binding.routingKey.foreach { routingKey =>
						channel.queueBind(binding.queue, binding.exchange, routingKey, binding.arguments.asJava)
					}
			})
			channel.close()
		}
	}

	private def createStream(killSwitch: SharedKillSwitch): Future[Done] = {

		val amqpSource = AmqpSource.committableSource(
			settings = settings,
			bufferSize = defaultPrefetchCount
		)
		amqpSource
			.map { msg =>
				logger.info("Message received")
				(msg, implicitly[Reads[A]].reads(msg.message.bytes.utf8String))
			}
			.via(killSwitch.flow)
			.throttle(900, 100.seconds, 10, ThrottleMode.Shaping)
			.mapAsync(20) {
				case (msg, Right(event)) =>
					config.processor(event).map(_ => msg.ack(false))(processorEx)
				case (msg, Left(ex)) =>
					ignore(msg.ack(false))
					logger.error(s"Invalid message: ${msg.message.bytes.utf8String}: ${ex.getClass.getName}: ${ex.getMessage}")
					Future.successful(Done)
			}.runWith(Sink.ignore)(materializer)
	}
}

object Subscription {

	final case object CheckBinding
	final case object Connect
	final case object Disconnect

	final case class Disconnected(result: Try[Done])

	def props[A: Reads](
		config: SubscriptionConfig[A],
		settings: NamedQueueSourceSettings,
		defaultPrefetchCount: Int,
		reconnectTime: FiniteDuration,
		logger: Slf4jLogger)(
		implicit
		materializer: Materializer,
		ex: ExecutionContext): Props = {

		Props(new Subscription(config, materializer, ex, settings, defaultPrefetchCount, reconnectTime, logger))
	}
}

