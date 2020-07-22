package com.kinja.amqp.impl.akkastream

import akka.Done
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.alpakka.amqp.{ AmqpConnectionProvider, AmqpWriteSettings, BindingDeclaration, ExchangeDeclaration, QueueDeclaration }
import com.github.sstone.amqp.Amqp.{ ExchangeParameters, QueueParameters }
import com.kinja.amqp.{ AmqpProducerInterface, QueueWithRelatedParameters, WithShutdown, extractErrorMessage, ignore }
import org.slf4j.LoggerFactory

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.{ Failure, Success, Try }

class ProducerTestFactory extends TestFactory {

	@SuppressWarnings(Array("org.wartremover.warts.Var"))
	private var consumers: List[TestConsumer] = List.empty[TestConsumer]

	@SuppressWarnings(Array("org.wartremover.warts.Var"))
	private var producers: List[AmqpProducerInterface with WithShutdown] = List.empty[AmqpProducerInterface with WithShutdown]

	def createTestConsumer[T](implicit materializer: Materializer): TestConsumer = {
		createTestConsumer[T](defaultQueueWithRelatedParameters)
	}

	val logger = LoggerFactory.getLogger("producer")

	override lazy val defaultQueueWithRelatedParameters: QueueWithRelatedParameters = {
		QueueWithRelatedParameters(
			queueParams = QueueParameters(
				name = "test-queue-producer",
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
			bindingKey = "test.producer.binding",
			deadLetterExchange = None
		)
	}

	def createTestConsumer[T](
		params: QueueWithRelatedParameters)(
		implicit
		materializer: Materializer): TestConsumer = {

		val exchangeDeclaration =
			ExchangeDeclaration(params.boundExchange.name, params.boundExchange.exchangeType)
				.withDurable(params.boundExchange.durable)
				.withAutoDelete(params.boundExchange.autodelete)
		val queueDeclaration = QueueDeclaration(params.queueParams.name)
			.withDurable(params.queueParams.durable)
			.withAutoDelete(params.queueParams.autodelete)
			.withExclusive(params.queueParams.exclusive)
		val bindingDeclaration =
			BindingDeclaration(params.queueParams.name, params.boundExchange.name).withRoutingKey(params.bindingKey)
		createTopology(connectionProvider, exchangeDeclaration, queueDeclaration, bindingDeclaration)

		val consumer: TestConsumer = new TestConsumer(
			connectionProvider = connectionProvider,
			queueDeclaration = queueDeclaration
		)
		consumers = consumer :: consumers
		consumer
	}

	def createTopology(
		connectionProvider: AmqpConnectionProvider,
		e: ExchangeDeclaration,
		qd: QueueDeclaration,
		binding: BindingDeclaration): Unit = {
		val topologyCheck =
			Try { connectionProvider.get }
				.map { connection =>
					if (connection.isOpen) {
						val channel = connection.createChannel()
						channel.exchangeDeclare(e.name, e.exchangeType, e.durable, e.autoDelete, e.arguments.asJava)
						channel.queueDeclare(qd.name, qd.durable, qd.exclusive, qd.autoDelete, qd.arguments.asJava)
						binding
							.routingKey
							.map(routingKey => channel.queueBind(binding.queue, binding.exchange, routingKey, binding.arguments.asJava))
						channel.close()
						connectionProvider.release(connection)
					}
				}

		topologyCheck match {
			case Success(_) =>
				logger.debug(s"Topology created!")
			case Failure(exception) =>
				logger.warn(s"Topology creation failed: ${extractErrorMessage(exception)}")
		}
	}

	def createAtMostOnceProducer(
		connectionProvider: AmqpConnectionProvider,
		queueParams: QueueWithRelatedParameters
	)(implicit system: ActorSystem, materializer: Materializer): AtMostOnceAmqpProducer = {
		val producer = new AtMostOnceAmqpProducer(
			connectionProvider = connectionProvider,
			logger = logger,
			system = system,
			materializer = materializer,
			params = queueParams.boundExchange
		)
		producers = producer :: producers
		producer
	}

	def createAtMostOnceProducer(implicit system: ActorSystem, materializer: Materializer): AtMostOnceAmqpProducer = {
		createAtMostOnceProducer(connectionProvider, defaultQueueWithRelatedParameters)
	}

	lazy val defaultConsumerConfig: ConsumerConfig = ConsumerConfig(
		connectionTimeOut = 1.second,
		shutdownTimeout = 5.seconds,
		defaultPrefetchCount = 10,
		reconnectionTime = 5.seconds,
		defaultParallelism = 4,
		defaultThrottling = Throttling(100, 1.seconds),
		defaultProcessingTimeout = 5.seconds
	)

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

	def shutdown(implicit ec: ExecutionContext): Future[Done] = {
		for {
			_ <- Future.sequence(producers.map(_.shutdown))
			_ <- Future.sequence(consumers.map(_.shutdown))
		} yield Done
	}

}
