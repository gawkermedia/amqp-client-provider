package com.kinja.amqp

import java.util.UUID

import com.kinja.amqp.model.Message
import com.kinja.amqp.model.MessageConfirmation

import scala.util.Try

trait MessageStore {

	def saveConfirmation(conf: MessageConfirmation): Unit
	def saveMessage(msg: Message): Try[Unit]
	def loadMessageOlderThan(time: Long): List[Message]
	def loadConfirmationByChannels(channelIds: List[UUID]): List[MessageConfirmation]
	def deleteMessage(id: Long): Unit
	def deleteConfirmation(conf: MessageConfirmation): Unit
}
