package com.kinja.amqp.model

case class MessageConfirmation(
	id: Option[Long],
	channelId: String,
	deliveryTag: Long,
	multiple: Boolean,
	createdTime: Long
)
