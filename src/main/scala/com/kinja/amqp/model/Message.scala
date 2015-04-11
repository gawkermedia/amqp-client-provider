package com.kinja.amqp.model

import java.util.UUID

case class Message(
	id: Option[Long],
	routingKey: String,
	exchangeName: String,
	message: String,
	channelId: Option[UUID],
	deliveryTag: Option[Long],
	createdTime: Long)