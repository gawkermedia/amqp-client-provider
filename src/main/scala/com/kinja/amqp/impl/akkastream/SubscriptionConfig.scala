package com.kinja.amqp.impl.akkastream

import com.kinja.amqp.Reads

import scala.concurrent.Future
import scala.concurrent.duration._

final case class SubscriptionConfig[A: Reads](
	prefetchCount: Int,
	timeout: FiniteDuration,
	throttling: Throttling,
	parallelism: Int,
	processor: A => Future[Unit]) {
}

final case class Throttling(elements: Int, per: FiniteDuration)

object SubscriptionConfig {

	def apply[A:Reads](
		timeout: FiniteDuration,
		prefetchCount: Option[Int],
		spacing: FiniteDuration,
		consumerConfig: ConsumerConfig)(
		processor: A => Future[Unit]): SubscriptionConfig[A] = {

		val throttling = Throttling(1, spacing)
		SubscriptionConfig[A](
			prefetchCount = consumerConfig.defaultPrefetchCount,
			timeout = timeout,
			throttling = throttling,
			parallelism =  consumerConfig.defaultParallelism,
			processor = processor)
	}

	def apply[A:Reads](
		timeout: FiniteDuration,
		spacing: FiniteDuration,
		consumerConfig: ConsumerConfig)(
		processor: A => Future[Unit]): SubscriptionConfig[A] = {

		apply[A](timeout, None, spacing, consumerConfig)(processor)
	}

	def apply[A:Reads](
		timeout: FiniteDuration,
		prefetchCount: Option[Int],
		consumerConfig: ConsumerConfig)(
		processor: A => Future[Unit]): SubscriptionConfig[A] = {

		SubscriptionConfig[A](
			prefetchCount = prefetchCount.getOrElse(consumerConfig.defaultPrefetchCount),
			timeout = timeout,
			throttling = consumerConfig.defaultThrottling,
			parallelism = consumerConfig.defaultParallelism,
			processor = processor
		)
	}

	def apply[A:Reads](
		timeout: FiniteDuration,
		consumerConfig: ConsumerConfig)(
		processor: A => Future[Unit]): SubscriptionConfig[A] = {

		apply[A](timeout, None, consumerConfig)(processor)
	}
}
