package com.kinja.amqp.impl.akkastream

import scala.concurrent.duration._

final case class ConsumerConfig(
	connectionTimeOut: FiniteDuration,
	shutdownTimeout: FiniteDuration,
	reconnectionTime: FiniteDuration,
	defaultPrefetchCount: Int,
	defaultParallelism: Int,
	defaultThrottling: Throttling,
	defaultProcessingTimeout: FiniteDuration)
