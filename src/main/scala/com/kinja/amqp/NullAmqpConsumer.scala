package com.kinja.amqp

import scala.concurrent.duration.FiniteDuration

class NullAmqpConsumer extends AmqpConsumerInterface {
	override def subscribe[A: Reads](processor: (A) => Unit): Unit = ()
	override def subscribe[A: Reads](spacing: FiniteDuration, processor: (A) => Unit): Unit = ()
}
