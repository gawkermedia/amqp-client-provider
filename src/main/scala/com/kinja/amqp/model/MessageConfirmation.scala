package com.kinja.amqp.model

import java.util.Date

case class MessageConfirmation(
	id: Option[Long],
	channelId: String,
	deliveryTag: Long,
	multiple: Boolean,
	createdTime: Date
)
