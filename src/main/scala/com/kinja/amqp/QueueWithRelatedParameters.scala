package com.kinja.amqp

import com.github.sstone.amqp.Amqp._

final case class QueueWithRelatedParameters(
	queueParams: QueueParameters,
	boundExchange: ExchangeParameters,
	deadLetterExchange: Option[ExchangeParameters],
	bindingKey: String
)
