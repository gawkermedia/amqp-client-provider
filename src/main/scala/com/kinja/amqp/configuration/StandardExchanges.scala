package com.kinja.amqp.configuration

object StandardExchanges {
	val amqDirect: ExchangeParameters = ExchangeParameters("amq.direct", passive = true, exchangeType = "direct")
	val amqFanout: ExchangeParameters = ExchangeParameters("amq.fanout", passive = true, exchangeType = "fanout")
	val amqTopic: ExchangeParameters = ExchangeParameters("amq.topic", passive = true, exchangeType = "topic")
	val amqHeaders: ExchangeParameters = ExchangeParameters("amq.headers", passive = true, exchangeType = "headers")
	val amqMatch: ExchangeParameters = ExchangeParameters("amq.match", passive = true, exchangeType = "headers")
}
