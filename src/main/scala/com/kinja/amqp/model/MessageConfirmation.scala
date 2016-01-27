package com.kinja.amqp.model

import java.sql.Timestamp

final case class MessageConfirmation(
	id: Option[Long],
	channelId: String,
	deliveryTag: Long,
	multiple: Boolean,
	createdTime: Timestamp
)
