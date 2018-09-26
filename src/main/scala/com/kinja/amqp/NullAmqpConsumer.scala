package com.kinja.amqp

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

class NullAmqpConsumer extends AmqpConsumerInterface {
	override def disconnect(): Unit = ()
	override def reconnect(): Unit = ()
	override def subscribe[A: Reads](timeout: FiniteDuration)(processor: (A) => Future[Unit]): Unit = ()
	override def subscribe[A: Reads](timeout: FiniteDuration, prefetchCount: Option[Int])(processor: (A) => Future[Unit]): Unit = ()
	override def subscribe[A: Reads](timeout: FiniteDuration, spacing: FiniteDuration, processor: (A) => Future[Unit]): Unit = ()
	override def subscribe[A: Reads](timeout: FiniteDuration, prefetchCount: Option[Int], spacing: FiniteDuration, processor: (A) => Future[Unit]): Unit = ()
}
