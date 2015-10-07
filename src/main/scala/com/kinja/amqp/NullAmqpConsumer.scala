package com.kinja.amqp

import play.api.libs.json.Reads

class NullAmqpConsumer extends AmqpConsumerInterface {
	override def subscribe[A: Reads](processor: (A) => Unit): Unit = {}
}