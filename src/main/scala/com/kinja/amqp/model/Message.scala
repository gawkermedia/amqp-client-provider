package com.kinja.amqp.model

import java.util.Date

case class Message(
	id: Option[Long],
	routingKey: String,
	exchangeName: String,
	message: String,
	channelId: Option[String],
	deliveryTag: Option[Long],
	createdTime: Date
)