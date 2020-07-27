package com.kinja.amqp.model

import java.util.UUID

final case class MessageWithContext[T](uuid: UUID, msgAsString: String, exchangeName: String, routingKey: String, response: T)
