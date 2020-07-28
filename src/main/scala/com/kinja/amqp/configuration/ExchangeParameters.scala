package com.kinja.amqp.configuration

/**
 * exchange parameters
 * @param name exchange name
 * @param passive if true, just check that the exchange exists
 * @param exchangeType exchange type: "direct", "fanout", "topic", "headers"
 * @param durable if true, the exchange will  be durable i.e. will survive a broker restart
 * @param autodelete if true, the exchange will be destroyed when it is no longer used
 * @param args additional arguments
 */
final case class ExchangeParameters(
	name: String,
	passive: Boolean,
	exchangeType: String,
	durable: Boolean = false,
	autodelete: Boolean = false,
	args: Map[String, AnyRef] = Map.empty[String, AnyRef])
