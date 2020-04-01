package com.kinja.amqp.impl.akkastream

import akka.Done
import akka.pattern.{ask, pipe}
import akka.actor.{Actor, ActorRef, ActorSystem, Props, Stash, Status, Timers}
import akka.stream.alpakka.amqp.scaladsl.AmqpSource
import akka.stream.alpakka.amqp.{AmqpConnectionProvider, BindingDeclaration, ExchangeDeclaration, NamedQueueSourceSettings, QueueDeclaration}
import akka.stream.scaladsl.Sink
import akka.stream.{KillSwitches, Materializer, SharedKillSwitch}
import akka.util.Timeout
import com.kinja.amqp.utils.Utils
import com.kinja.amqp.{AmqpConsumerInterface, QueueWithRelatedParameters, Reads, WithShutdown, ignore}
import org.slf4j.{Logger => Slf4jLogger}

import scala.jdk.CollectionConverters._
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

class AmqpConsumer(
	connectionProvider: AmqpConnectionProvider,
	logger: Slf4jLogger,
	system: ActorSystem,
	materializer: Materializer,
	consumerConfig: ConsumerConfig)(
	params: QueueWithRelatedParameters) extends AmqpConsumerInterface with WithShutdown{

	private implicit val askTimeOut: Timeout = Timeout(consumerConfig.shutdownTimeout + 100.millis)

	@SuppressWarnings(Array("org.wartremover.warts.Var"))
	private var subscription: Option[ActorRef] = None
	/**
	 * Stops processing messages from now on, effectively unsubscribing from the queue.
	 * If no subscription was performed before, it does nothing.
	 */
	override def disconnect(): Unit = {
		subscription.foreach(_ ! Subscription.Disconnect)
	}

	/**
	 * Resumes processing messages from now on, as if resubscribed to the queue.
	 * If no subscription was performed before, it does nothing.
	 */
	override def reconnect(): Unit = {
		subscription.foreach(_ ! Subscription.Connect)
	}

	/**
	 * Subscribes the message processor function to consume the queue described by params.
	 *
	 * @param timeout   The maximum amount of time to wait for processing to complete.
	 * @param processor The pmessage processor function.
	 */
	override def subscribe[A: Reads](timeout: FiniteDuration)(processor: A => Future[Unit]): Unit = {
		val config = SubscriptionConfig[A](timeout,consumerConfig)(processor)
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
		val config = SubscriptionConfig[A](timeout, prefetchCount, consumerConfig)(processor)
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
		val config = SubscriptionConfig[A](timeout, spacing, consumerConfig)(processor)
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
		val config = SubscriptionConfig[A](timeout, prefetchCount, spacing, consumerConfig)(processor)
		createSubscription(config)
	}

	private def createSubscription[A: Reads](subscriptionConfig: SubscriptionConfig[A]): Unit = {
		subscription = subscription match {
			case s @ Some(_) =>
				logger.warn(s"MessageConsumer for ${params.queueParams.name} already subscribed!")
				s
			case None =>
				implicit val mat: Materializer = materializer
				val subscription = system.actorOf(
					props = Subscription.props[A](
						config = subscriptionConfig,
						settings = baseSettings,
						reconnectTime = 1.seconds,
						logger = logger
					),
					name = s"ampqp_consumer_${baseSettings.queue}"
				)
				subscription ! Subscription.Connect
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

	override def shutdown: Future[Done] = {
		subscription match {
			case Some(subscriptionActor) =>
				(subscriptionActor ? Subscription.ShutDown).mapTo[Done]
			case None => Future.successful(Done)
		}
	}
}

final class Subscription[A: Reads](
	config: SubscriptionConfig[A],
	materializer: Materializer,
	settings: NamedQueueSourceSettings,
	reconnectTime: FiniteDuration,
	logger: Slf4jLogger) extends Actor with Stash with Timers {

	import context.dispatcher
	private val superVisorName = s"ampqp_consumer_${settings.queue}"

	override def preStart(): Unit = {
		checkTopology()
		timers.startTimerWithFixedDelay("check-queue", Subscription.CheckTopology, 5.seconds)
		super.preStart()
	}

	override def receive: Receive = disconnected

	private def disconnected: Receive = {
		case Subscription.Connect =>
			val killSwitch = KillSwitches.shared(superVisorName)
			val streamDone = createStream(killSwitch)
			ignore(streamDone pipeTo self)
			context.become(connected(killSwitch, streamDone))
		case Subscription.CheckTopology =>
			checkTopology()
		case Subscription.ShutDown =>
			logger.warn(s"[$superVisorName]: Shutting down disconnected stream!")
			context.become(shuttingDown(sender()))
			self ! Done
	}

	@SuppressWarnings(Array("org.wartremover.warts.IsInstanceOf"))
	private def connected(killSwitch: SharedKillSwitch, streamDone: Future[Done]): Receive = {
		case Subscription.Disconnect =>
			killSwitch.shutdown()
			ignore(streamDone pipeTo sender())
			context.become(disconnected)
		case Status.Failure(ex) =>
			logger.error(s"[$superVisorName]: Stream failed with: ${ex.getClass.getName}: ${ex.getMessage}. Reconnecting!")
			timers.startSingleTimer(s"reconnecting_$superVisorName", Subscription.Connect, reconnectTime)
			context.become(disconnected)
		case Status.Success(_) =>
			logger.warn(s"[$superVisorName]: Stream finished!")
			context.become(disconnected)
		case Subscription.CheckTopology =>
			checkTopology()
		case Subscription.ShutDown =>
			logger.warn(s"[$superVisorName]: Shutting down connected stream!")
			killSwitch.shutdown()
			timers.startSingleTimer(s"shutdown_timedOut_$superVisorName", Subscription.Connect, reconnectTime)
			context.become(shuttingDown(sender()))
	}

	private def shuttingDown(requester: ActorRef): Receive = {
		case Done =>
			logger.warn(s"[$superVisorName]: Shut down is Done!")
			timers.cancelAll()
			requester ! Done
			context.stop(self)
		case Status.Success(_) =>
			logger.warn(s"[$superVisorName]: Shut down is Succeed!")
			timers.cancelAll()
			requester ! Done
			context.stop(self)
		case Status.Failure(ex) =>
			logger.error(s"[$superVisorName]: Shut down is Failed: ${ex.getClass.getName}: ${ex.getMessage}")
			timers.cancelAll()
			ignore(Future.failed[Done](ex) pipeTo requester)
			context.stop(self)
	}

	//Idempotent recreation of the topology if something is missing.
	private def checkTopology() = {
		Try{settings.connectionProvider.get} match {
			case Success(connection) =>
				if (connection.isOpen) {
					val channel = connection.createChannel()
					ignore(settings.declarations.collectFirst {
						case e: ExchangeDeclaration =>
							logger.debug(s"[$superVisorName]: Declaring exchange: ${e.name}")
							channel.exchangeDeclare(e.name, e.exchangeType, e.durable, e.autoDelete, e.arguments.asJava)
					})
					ignore(settings.declarations.collectFirst {
						case qd: QueueDeclaration =>
							logger.debug(s"[$superVisorName]: Declaring queue: ${qd.name}")
							channel.queueDeclare(qd.name, qd.durable, qd.exclusive, qd.autoDelete, qd.arguments.asJava)
					})
					ignore(settings.declarations.collectFirst {
						case binding: BindingDeclaration =>
							binding.routingKey.foreach { routingKey =>
								logger.debug(s"[$superVisorName]: Declaring binding: ${binding.exchange} ---(${routingKey})--->${binding.queue}")
								channel.queueBind(binding.queue, binding.exchange, routingKey, binding.arguments.asJava)
							}
					})
					channel.close()
					settings.connectionProvider.release(connection)
				}
			case Failure(exception) =>
				logger.error(s"[$superVisorName]: Topology check failed: Unable to get connection!", exception)
		}
	}

	@SuppressWarnings(Array("org.wartremover.warts.ToString"))
	private def createStream(killSwitch: SharedKillSwitch): Future[Done] = {
		val amqpSource = AmqpSource.committableSource(
			settings = settings,
			bufferSize = config.prefetchCount
		)
		amqpSource
			.map { msg =>
				(msg, implicitly[Reads[A]].reads(msg.message.bytes.utf8String))
			}
			.via(killSwitch.flow)
			.throttle(config.throttling.elements, config.throttling.per)
			.mapAsync(config.parallelism) {
				case (msg, Right(event)) =>
					logger.debug(s"[$superVisorName]: Message Received: ${event.toString}")
					Utils
						.withTimeout(
							name ="processor",
							step = config.processor(event).map[Acknowledgment](_ => Acknowledgment.Ack(msg)),
							timeout = config.timeout)(
							actorSystem = context.system
						)
						.recover[Acknowledgment] {
							case NonFatal(ex) =>
								logger.error(s"[$superVisorName]: Message processing failed: ${event.toString} with ${ex.getClass.getName}: ${ex.getMessage}")
								Acknowledgment.Nack(msg)
						}
				case (msg, Left(ex)) =>
					logger.error(s"[$superVisorName]: Invalid message: ${msg.message.bytes.utf8String}: ${ex.getClass.getName}: ${ex.getMessage}")
					Future.successful(Acknowledgment.Ack(msg))
			}
			.mapAsync(config.parallelism){
				case Acknowledgment.Ack(msg) => msg.ack()
				case Acknowledgment.Nack(msg) => msg.nack()
			}
			.runWith(Sink.ignore)(materializer)
	}
}

object Subscription {

	final case object CheckTopology
	final case object Connect
	final case object Disconnect
	final case object ShutDown

	final case class Disconnected(result: Try[Done])

	def props[A: Reads](
		config: SubscriptionConfig[A],
		settings: NamedQueueSourceSettings,
		reconnectTime: FiniteDuration,
		logger: Slf4jLogger)(
		implicit materializer: Materializer): Props = {

		Props(new Subscription(config, materializer, settings, reconnectTime, logger))
	}
}

