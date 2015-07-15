package com.kinja.amqp.model

import java.sql.Timestamp

case class MessageConfirmation(
	id: Option[Long],
	channelId: String,
	deliveryTag: Long,
	multiple: Boolean,
	createdTime: Timestamp)
