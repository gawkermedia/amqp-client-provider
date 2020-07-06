package com.kinja.amqp.impl.akkastream

import akka.actor.ActorSystem
import akka.{ Done, NotUsed }
import akka.stream.{ Attributes, FlowShape, Materializer, OverflowStrategy, QueueOfferResult }
import akka.stream.alpakka.amqp._
import akka.stream.alpakka.amqp.scaladsl.AmqpFlow
import akka.stream.scaladsl.{ Flow, GraphDSL, Keep, RestartFlow, Sink, Source, SourceQueueWithComplete, Unzip, Zip }
import akka.util.ByteString
import com.github.sstone.amqp.Amqp.ExchangeParameters
import com.kinja.amqp.{ WithShutdown, Writes }
import org.slf4j.Logger

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{ Failure, Success }

class AtMostOnceAmqpProducer(
	connectionProvider: AmqpConnectionProvider,
	logger: Logger,
	system: ActorSystem,
	materializer: Materializer,
	params: ExchangeParameters) extends WithShutdown {

	type Context = TimedPromise[WriteResult]
	type WithContext[T] = (T, Context)

	private implicit val ec = system.dispatcher

	def publish[A: Writes](
		routingKey: String,
		message: A,
		saveTimeMillis: Long = System.currentTimeMillis()): Future[Unit] = {
		val messageString = implicitly[Writes[A]].writes(message)
		logger.debug(s"Message to send: $messageString")
		val bytes = ByteString(messageString)
		val msg = WriteMessage(bytes).withRoutingKey(routingKey)
		send(msg)
	}

	private def createWriteSettings(params: ExchangeParameters): AmqpWriteSettings = {
		AmqpWriteSettings(connectionProvider)
			.withExchange(params.name)
			.withBufferSize(10)
			.withConfirmationTimeout(200.millis)
			.withDeclarations(
				Seq(
					ExchangeDeclaration(params.name, params.exchangeType)
						.withDurable(params.durable)
						.withAutoDelete(params.autodelete)
						.withArguments(params.args)
				)
			)
	}

	private lazy val settings = createWriteSettings(params)

	def amqpFlowWithContext: Flow[WithContext[WriteMessage], WithContext[WriteResult], NotUsed] =
		Flow.fromGraph(GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
			import GraphDSL.Implicits._
			val unzip = builder.add(Unzip[WriteMessage, Context])
			val amqpFlow = builder.add(AmqpFlow.withConfirm(settings))
			val zip = builder.add(Zip[WriteResult, Context])
			unzip.out0 ~> amqpFlow ~> zip.in0
			unzip.out1 ~> zip.in1
			FlowShape(unzip.in, zip.out)
		})

	private def faultTolerantAmqpFlow =
		RestartFlow
			.onFailuresWithBackoff[WithContext[WriteMessage], WithContext[WriteResult]](
				minBackoff = 100.millis,
				maxBackoff = 2.seconds,
				randomFactor = 0.3D,
				maxRestarts = -1
			)(() => amqpFlowWithContext)

	private def sourceWithContext =
		Source
			.queue[WithContext[WriteMessage]](10, OverflowStrategy.backpressure)

	private def createStream: (SourceQueueWithComplete[WithContext[WriteMessage]], Future[Done]) = {
		sourceWithContext
			.addAttributes(
				Attributes.logLevels(
					onElement = Attributes.LogLevels.Info,
					onFinish = Attributes.LogLevels.Info,
					onFailure = Attributes.LogLevels.Error))
			.via(faultTolerantAmqpFlow)
			.map { case (result, responsePromise) => responsePromise.success(result) }
			.toMat(Sink.ignore)(Keep.both)
			.run()(materializer)
	}

	private val (queue, streamDone) = createStream

	streamDone.onComplete {
		case Success(_) => logger.info(s"ProducerStream completed with: Done")
		case Failure(exception) => logger.warn(s"ProducerStream completed with failure", exception)
	}

	private def send(msg: WriteMessage): Future[Unit] = {
		val p = TimedPromise.create[WriteResult](system, 5.seconds)
		for {
			offerResult <- queue.offer((msg, p))
			result <- offerResult match {
				case QueueOfferResult.Enqueued =>
					p.future.flatMap {
						case r if r.confirmed => Future.successful(())
						case _ => Future.failed[Unit](new IllegalStateException("Message was rejected"))
					}
				case QueueOfferResult.Failure(cause) =>
					Future.failed[Unit](new IllegalStateException("Queue offer failed with exception:", cause))
				case QueueOfferResult.Dropped =>
					Future.failed[Unit](new IllegalStateException("Queue offer failed: Item dropped"))
				case QueueOfferResult.QueueClosed =>
					Future.failed[Unit](new IllegalStateException("Queue offer failed: Queue already closed!"))
			}
		} yield result
	}

	def shutdown: Future[Done] = {
		queue.complete()
		streamDone
	}
}
