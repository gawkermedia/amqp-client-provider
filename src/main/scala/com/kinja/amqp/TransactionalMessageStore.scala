package com.kinja.amqp

import com.kinja.amqp.model.Message
import com.kinja.amqp.model.MessageConfirmation

/**
 * The TransactionalMessageStore should make sure that
 * the loaded records between start and commit are not updated/deleted by any other transaction.
 * If it can not be guaranteed AND multiple message repeater instances exist,
 * same repeaters may try to resend or delete the very same messages/confirmations at a given time
 */
trait TransactionalMessageStore {

	def start: Unit

	def commit: Unit

	def loadMessageOlderThan(time: Long, exchangeName: String, limit: Int): List[Message]

	def loadConfirmationByChannels(channelIds: List[String]): List[MessageConfirmation]

	def deleteMessage(id: Long): Unit

	def deleteConfirmation(id: Long): Unit
}