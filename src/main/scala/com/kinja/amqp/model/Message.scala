package com.kinja.amqp.model

import java.sql.Date

case class Message(
	id: Option[Long],
	routingKey: String,
	exchangeName: String,
	message: String,
	channelId: Option[String],
	deliveryTag: Option[Long],
	createdTime: Date,
	processedBy: Option[String] = None,
	lockedAt: Option[Date] = None
)