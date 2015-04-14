package com.kinja.amqp.model

case class Message(
	id: Option[Long],
	routingKey: String,
	exchangeName: String,
	message: String,
	channelId: Option[String],
	deliveryTag: Option[Long],
	createdTime: Long
)