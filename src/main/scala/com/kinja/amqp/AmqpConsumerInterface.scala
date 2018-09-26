package com.kinja.amqp

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

trait AmqpConsumerInterface {

	/**
	 * Stops processing messages from now on, effectively unsubscribing from the queue.
	 * If no subscription was performed before, it does nothing.
	 */
	def disconnect(): Unit

	/**
	 * Resumes processing messages from now on, as if resubscribed to the queue.
	 * If no subscription was performed before, it does nothing.
	 */
	def reconnect(): Unit

	/**
	 * Subscribes the message processor function to consume the queue described by params.
	 * @param timeout The maximum amount of time to wait for processing to complete.
	 * @param processor The pmessage processor function.
	 */
	def subscribe[A: Reads](timeout: FiniteDuration)(processor: A => Future[Unit]): Unit

	/**
	 * Subscribes the message processor function to consume the queue described by params.
	 * @param timeout The maximum amount of time to wait for processing to complete.
	 * @param prefetchCount The maximum amount of "in flight" messages.
	 *        If not set to None the prefetch count will be unlimited.
	 *        Default value set to 10.
	 *        (https://www.rabbitmq.com/amqp-0-9-1-reference.html#basic.qos.prefetch-count)
	 * @param processor The message processor function.
	 */
	def subscribe[A: Reads](
		timeout: FiniteDuration,
		prefetchCount: Option[Int])(processor: A => Future[Unit]): Unit

	/**
	 * Subscribes the message processor function to consume the queue described by params.
	 * @param timeout The maximum amount of time to wait for processing to complete.
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
	def subscribe[A: Reads](timeout: FiniteDuration, spacing: FiniteDuration, processor: A => Future[Unit]): Unit

	/**
	 * Subscribes the message processor function to consume the queue described by params.
	 * @param timeout The maximum amount of time to wait for processing to complete.
	 * @param prefetchCount The maximum amount of "in flight" messages.
	 *        If not set the prefetch count will be unlimited.
	 *        If spacing set greater than zero this value overridden with 1.
	 *        Default value is 10.
	 *        (https://www.rabbitmq.com/amqp-0-9-1-reference.html#basic.qos.prefetch-count)
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
	def subscribe[A: Reads](
		timeout: FiniteDuration,
		prefetchCount: Option[Int],
		spacing: FiniteDuration,
		processor: A => Future[Unit]): Unit
}
