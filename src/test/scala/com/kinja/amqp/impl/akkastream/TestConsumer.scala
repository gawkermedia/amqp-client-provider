package com.kinja.amqp.impl.akkastream

import akka.{ Done, NotUsed }
import akka.stream.Materializer
import akka.stream.alpakka.amqp._
import akka.stream.alpakka.amqp.scaladsl.AmqpSource
import akka.stream.scaladsl.{ Sink, Source }
import com.kinja.amqp.Reads
import org.slf4j.LoggerFactory

import scala.concurrent.Future

class TestConsumer(
	connectionProvider: AmqpConnectionProvider,
	queueDeclaration: QueueDeclaration)(
	implicit
	materializer: Materializer) {

	val logger = LoggerFactory.getLogger("testProducer")

	def amqpSource(): Source[ReadResult, NotUsed] =
		AmqpSource.atMostOnceSource(
			NamedQueueSourceSettings(connectionProvider, queueDeclaration.name)
				.withDeclaration(queueDeclaration)
				.withAckRequired(false),
			bufferSize = 10
		)

	def take[T: Reads](size: Long): Future[Seq[Either[Throwable, T]]] =
		amqpSource()
			.take(size)
			.map(result => implicitly[Reads[T]].reads(result.bytes.utf8String))
			.runWith(Sink.seq)

	def shutdown: Future[Done] = Future.successful(Done)
}
