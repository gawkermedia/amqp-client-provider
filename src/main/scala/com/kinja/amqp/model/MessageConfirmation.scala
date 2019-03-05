package com.kinja.amqp.model

import java.sql.Timestamp

final case class MessageConfirmation(
	channelId: String,
	deliveryTag: Long,
	multiple: Boolean,
	createdTime: Timestamp
)
