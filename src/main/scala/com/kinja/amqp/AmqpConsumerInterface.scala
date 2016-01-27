package com.kinja.amqp

import scala.concurrent.duration.FiniteDuration

trait AmqpConsumerInterface {

	/**
	 * Subscribes the message processor function to consume the queue described by params.
	 * @param processor The pmessage processor function.
	 */
	def subscribe[A: Reads](processor: A => Unit): Unit

	/**
	 * Subscribes the message processor function to consume the queue described by params.
	 * @param spacing The minimum amount of time that has to elapse between starting processing
	 *        new messages. It can be used to define rate limiting, for example, setting 10
	 *        seconds here means that only one message may be processed each 10 seconds, resulting
	 *        in a processing rate of 0.1 message/sec. Note that this is not the time to wait
	 *        between processing messages (end of last and beginning of next) but rather
	 *        the time between the start of each processing. This means, sticking to the previous
	 *        example, that if processing took more than 10 seconds, processing the next message
	 *        can immediately be started.
	 * @param processor The pmessage processor function.
	 */
	def subscribe[A: Reads](spacing: FiniteDuration, processor: A => Unit): Unit
}
