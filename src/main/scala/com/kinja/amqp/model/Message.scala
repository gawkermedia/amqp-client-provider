package com.kinja.amqp.model

import java.sql.Timestamp

sealed trait MessageLike {
	def routingKey: String
	def exchangeName: String
	def message: String
	def createdTime: Timestamp
}

final case class Message(
	routingKey: String,
	exchangeName: String,
	message: String,
	channelId: String,
	deliveryTag: Long,
	createdTime: Timestamp
) extends MessageLike

final case class FailedMessage(
	id: Option[Long],
	routingKey: String,
	exchangeName: String,
	message: String,
	createdTime: Timestamp
) extends MessageLike
