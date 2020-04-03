package com.kinja.amqp.impl.akkastream

import akka.stream.alpakka.amqp
import akka.{ Done, NotUsed }
import akka.stream.{ FlowShape, Materializer, OverflowStrategy, QueueOfferResult }
import akka.stream.alpakka.amqp.scaladsl.AmqpFlow
import akka.stream.alpakka.amqp.{ AmqpWriteSettings, WriteMessage, WriteResult }
import akka.stream.scaladsl.{ Flow, GraphDSL, Keep, Sink, Source, Unzip, Zip }
import akka.util.ByteString
import com.kinja.amqp.Writes
import org.slf4j.LoggerFactory

import scala.concurrent.{ ExecutionContext, Future, Promise }

class TestProducer[T](
	settings: AmqpWriteSettings,
	materializer: Materializer)(
	implicit
	writes: Writes[T],
	ex: ExecutionContext) {

	val logger = LoggerFactory.getLogger("testProducer")

	//: Flow[WriteMessage, WriteResult, Future[Done]]
	def amqpFlowWithContext[Ctx]: Flow[(WriteMessage, Ctx), (WriteResult, Ctx), NotUsed] =
		Flow.fromGraph(GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
			import GraphDSL.Implicits._
			val unzip = builder.add(Unzip[WriteMessage, Ctx])
			val amqpFlow = builder.add(AmqpFlow.withConfirm(settings))
			val zip = builder.add(Zip[WriteResult, Ctx])
			unzip.out0 ~> amqpFlow ~> zip.in0
			unzip.out1 ~> zip.in1
			FlowShape(unzip.in, zip.out)
		})

	private val sourceWithContext =
		Source
			.queue[(T, Promise[WriteResult])](1, OverflowStrategy.backpressure)
			.map {
				case (msg, ctx) =>
					val asString: String = writes.writes(msg)
					(WriteMessage(ByteString(asString)), ctx)
			}

	private val (queue, streamDone) = {
		sourceWithContext
			.via(amqpFlowWithContext[Promise[WriteResult]])
			.map { case (result, responsePromise) => responsePromise.success(result) }
			.toMat(Sink.ignore)(Keep.both)
			.run()(materializer)
	}

	streamDone.onComplete {
		s => logger.debug(s"Producer stream completed with: $s")
	}

	def send(msg: T): Future[WriteResult] = {
		val p = Promise[WriteResult]
		for {
			offerResult <- queue.offer((msg, p))
			result <- offerResult match {
				case QueueOfferResult.Enqueued =>
					p.future
				case QueueOfferResult.Failure(cause) =>
					Future.failed[WriteResult](new IllegalStateException("Queue offer failed with exception:", cause))
				case QueueOfferResult.Dropped =>
					Future.failed[amqp.WriteResult](new IllegalStateException("Queue offer failed: Item dropped"))
				case QueueOfferResult.QueueClosed =>
					Future.failed[amqp.WriteResult](new IllegalStateException("Queue offer failed: Queue already closed!"))
			}
		} yield result
	}

	def shutdown: Future[Done] = {
		queue.complete()
		streamDone
	}

}
