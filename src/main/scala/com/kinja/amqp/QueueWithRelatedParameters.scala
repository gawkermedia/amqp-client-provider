package com.kinja.amqp

import com.github.sstone.amqp.Amqp._

case class QueueWithRelatedParameters(
	queueParams: QueueParameters,
	boundExchange: ExchangeParameters,
	deadLetterExchange: Option[ExchangeParameters],
	bindingKey: String
)