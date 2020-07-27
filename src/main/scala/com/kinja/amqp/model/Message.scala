package com.kinja.amqp.model

import java.sql.Timestamp
import java.util.UUID

final case class FailedMessage(
	id: Option[Long],
	routingKey: String,
	exchangeName: String,
	message: String,
	messageId: UUID,
	createdTime: Timestamp
)
