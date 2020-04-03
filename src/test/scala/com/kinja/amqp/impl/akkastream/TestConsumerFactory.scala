package com.kinja.amqp.impl.akkastream

import akka.Done
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.alpakka.amqp._
import com.github.sstone.amqp.Amqp.{ ExchangeParameters, QueueParameters }
import com.kinja.amqp.{ QueueWithRelatedParameters, Writes }
import com.rabbitmq.client.ConnectionFactory
import org.slf4j.LoggerFactory

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._

class TestConsumerFactory {

	@SuppressWarnings(Array("org.wartremover.warts.Var"))
	private var consumers: List[AmqpConsumer] = List.empty[AmqpConsumer]

	@SuppressWarnings(Array("org.wartremover.warts.Var"))
	private var producers: List[TestProducer[_]] = List.empty[TestProducer[_]]

	lazy val connectionFactory: ConnectionFactory = {
		val factory = new ConnectionFactory()
		factory.setUsername("guest")
		factory.setPassword("guest")
		factory.setRequestedHeartbeat(60)
		factory.setConnectionTimeout(100.millis.toMillis.toInt)
		factory.setAutomaticRecoveryEnabled(false)
		factory
	}

	lazy val hostAndPorts: Seq[(String, Int)] = Seq(("rabbit-gmginfra", 5672))

	lazy val connectionProvider = createConnectionProvider(connectionFactory, hostAndPorts)

	def createConsumer(implicit system: ActorSystem, materializer: Materializer): AmqpConsumer = {
		createConsumer(defaultConsumerConfig, defaultQueueWithRelatedParameters)
	}

	def createConsumer(
		consumerConfig: ConsumerConfig,
		queueWithRelatedParameters: QueueWithRelatedParameters)(
		implicit
		system: ActorSystem,
		materializer: Materializer): AmqpConsumer = {

		val logger = LoggerFactory.getLogger("consumer")
		val consumer = new AmqpConsumer(
			connectionProvider = connectionProvider,
			logger = logger,
			system = system,
			materializer = materializer,
			consumerConfig = consumerConfig
		)(params = queueWithRelatedParameters)
		consumers = consumer :: consumers
		consumer
	}

	def createTestProducer[T](
		connectionProvider: AmqpConnectionProvider,
		queueParams: QueueWithRelatedParameters
	)(implicit writes: Writes[T], system: ActorSystem, materializer: Materializer): TestProducer[T] = {
		val settings = createWriteSettings(connectionProvider, queueParams)
		val producer = new TestProducer[T](settings, materializer)(writes, system.dispatcher)
		producers = producer :: producers
		producer
	}

	def createTestProducer[T](implicit writes: Writes[T], system: ActorSystem, materializer: Materializer): TestProducer[T] = {
		createTestProducer(connectionProvider, defaultQueueWithRelatedParameters)
	}

	private def createConnectionProvider(factory: ConnectionFactory, hostsAndPorts: Seq[(String, Int)]) = AmqpCachedConnectionProvider(
		AmqpConnectionFactoryConnectionProvider(
			factory,
		).withHostsAndPorts(hostsAndPorts)
	)

	lazy val defaultConsumerConfig: ConsumerConfig = ConsumerConfig(
		connectionTimeOut = 1.second,
		shutdownTimeout = 5.seconds,
		defaultPrefetchCount = 10,
		reconnectionTime = 5.seconds,
		defaultParallelism = 4,
		defaultThrottling = Throttling(100, 1.seconds),
		defaultProcessingTimeout = 5.seconds
	)

	lazy val defaultQueueWithRelatedParameters: QueueWithRelatedParameters = {
		QueueWithRelatedParameters(
			queueParams = QueueParameters(
				name = "test-queue",
				passive = false,
				durable = true,
				exclusive = false,
				autodelete = false,
				args = Map.empty[String, AnyRef]
			),
			boundExchange = ExchangeParameters(
				name = "test-exchange",
				passive = false,
				exchangeType = "topic",
				durable = true
			),
			bindingKey = "test.binding",
			deadLetterExchange = None
		)
	}

	def createWriteSettings(connectionProvider: AmqpConnectionProvider, params: QueueWithRelatedParameters) = {
		AmqpWriteSettings(connectionProvider)
			.withExchange(params.boundExchange.name)
			.withRoutingKey(params.bindingKey)
			.withBufferSize(10)
			.withConfirmationTimeout(200.millis)
			.withDeclarations(
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

	def deleteBindingQueueAndExchange(queueParameters: QueueWithRelatedParameters): Unit = {
		val connection = connectionProvider.get
		val channel = connection.createChannel()
		channel.queueUnbind(queueParameters.queueParams.name, queueParameters.boundExchange.name, queueParameters.bindingKey)
		channel.queueDelete(queueParameters.queueParams.name)
		channel.exchangeDelete(queueParameters.boundExchange.name)
		channel.close()
		connectionProvider.release(connection)
	}

	def shutdown(implicit ec: ExecutionContext): Future[Done] = {
		for {
			_ <- Future.sequence(producers.map(_.shutdown))
			_ <- Future.sequence(consumers.map(_.shutdown))
		} yield Done
	}

}
