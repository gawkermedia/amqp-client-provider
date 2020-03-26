package com.kinja.amqp.impl.akkastream

import com.kinja.amqp.Reads

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

final case class SubscriptionConfig[A: Reads](
	timeout: FiniteDuration,
	prefetchCount: Option[Int] = None,
	spacing: Option[FiniteDuration] = None,
	processor: A => Future[Unit]) {
}
