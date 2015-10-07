package com.kinja.amqp

import play.api.libs.json.Reads

trait AmqpConsumerInterface {
	def subscribe[A: Reads](processor: A => Unit): Unit
}
