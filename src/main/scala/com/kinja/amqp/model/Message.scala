package com.kinja.amqp.model

import java.sql.Timestamp

sealed trait MessageLike {
	def routingKey: String
	def exchangeName: String
	def message: String
	def createdTime: Timestamp
}

final case class Message(
	override val routingKey: String,
	override val exchangeName: String,
	override val message: String,
	channelId: String,
	deliveryTag: Long,
	override val createdTime: Timestamp
) extends MessageLike

final case class FailedMessage(
	id: Option[Long],
	override val routingKey: String,
	override val exchangeName: String,
	override val message: String,
	override val createdTime: Timestamp
) extends MessageLike
