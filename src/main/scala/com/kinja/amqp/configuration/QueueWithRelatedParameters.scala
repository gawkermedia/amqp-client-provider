package com.kinja.amqp.configuration

final case class QueueWithRelatedParameters(
	queueParams: QueueParameters,
	boundExchange: ExchangeParameters,
	deadLetterExchange: Option[ExchangeParameters],
	bindingKey: String
)
