package com.kinja.amqp.model

import java.util.UUID

case class MessageConfirmation(
	id: Option[Long],
	channelId: UUID,
	deliveryTag: Long,
	multiple: Boolean)
