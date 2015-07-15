package com.kinja.amqp.model

import java.sql.Timestamp

case class Message(
	id: Option[Long],
	routingKey: String,
	exchangeName: String,
	message: String,
	channelId: Option[String],
	deliveryTag: Option[Long],
	createdTime: Timestamp,
	processedBy: Option[String] = None,
	lockedAt: Option[Timestamp] = None)