package com.kinja.amqp.persistence

import java.sql.Timestamp

import akka.actor.{ Actor, ActorLogging }
import akka.event.LoggingReceive
import com.kinja.amqp.model.{ Message, MessageConfirmation }
import org.slf4j.{ Logger => Slf4jLogger }

import scala.collection.mutable.{ ArrayBuffer, Map => MutableMap }

case class SaveMessage(message: Message)
case class MultipleConfirmation(confirm: MessageConfirmation)
case class DeleteMessageUponConfirm(channelId: String, deliveryTag: Long)
case class RemoveMessagesOlderThan(milliSeconds: Long)
case object RemoveMultipleConfirmations
case class LogBufferStatistics(logger: Slf4jLogger)

class InMemoryMessageBuffer extends Actor with ActorLogging {

	val messageBuffer: ArrayBuffer[Message] = ArrayBuffer()
	val confirmations: MutableMap[String, Long] = MutableMap()

	private def handleMultipleConfirmation(confirm: MessageConfirmation): Unit = {
		if (!confirm.multiple) {
			throw new IllegalStateException("Got single confirm instead of multiple")
		}

		val messagesToDelete: ArrayBuffer[Message] = messageBuffer.filter(message =>
			message.channelId == Some(confirm.channelId) &&
				message.deliveryTag.getOrElse(Long.MaxValue) < confirm.deliveryTag)

		messagesToDelete.foreach { message =>
			messageBuffer -= message
		}

		confirmations.update(confirm.channelId, confirm.deliveryTag)
	}

	private def saveMessage(message: Message): Unit = messageBuffer += message

	private def deleteMessageUponConfirm(channelId: String, deliveryTag: Long): Unit = {
		val messageToDelete: Option[Message] = messageBuffer.find(message =>
			message.channelId == Some(channelId) && message.deliveryTag == Some(deliveryTag))

		messageToDelete.foreach { message =>
			messageBuffer -= message
		}

		sender ! messageToDelete.isDefined
	}

	private def removeMessageOlderThan(milliSeconds: Long): Unit = {
		val currentMillis: Long = System.currentTimeMillis()
		val removeBeforeDate = new Timestamp(currentMillis - milliSeconds)
		val messagesToRemove = messageBuffer.filter { message =>
			message.createdTime.before(removeBeforeDate)
		}.toList

		//		println(s"Current millis are $currentMillis, which is ${new Timestamp(currentMillis)}")
		//		println(s"I'll remove these messages, which are before date $removeBeforeDate")
		//		println(messagesToRemove)

		messagesToRemove.foreach { message =>
			messageBuffer -= message
		}

		sender ! messagesToRemove
	}

	private def removeMultipleConfirmations(): Unit = {
		sender ! confirmations.toMap

		confirmations.clear()
	}

	private def logBufferStatistics(logger: Slf4jLogger): Unit = {
		val time = System.currentTimeMillis()
		val averageAge = if (messageBuffer.nonEmpty) {
			val time = System.currentTimeMillis()
			val allAge = messageBuffer.foldLeft(0L) { (acc: Long, message: Message) =>
				acc + (time - message.createdTime.getTime)
			}
			allAge / messageBuffer.size
		} else {
			0
		}

		val maxAge = messageBuffer.foldLeft(0L) { (acc: Long, message: Message) =>
			val age = time - message.createdTime.getTime
			Math.max(acc, age)
		}

		logger.info(s"There are ${messageBuffer.size} message in the buffer, " +
			s"with average age of $averageAge millis, " +
			s"max age is $maxAge millis")
		logger.info(s"There are ${confirmations.size} confirmations in the buffer, and they are $confirmations")
	}

	override def receive: Receive = LoggingReceive {
		case SaveMessage(message) => saveMessage(message)
		case MultipleConfirmation(confirm) => handleMultipleConfirmation(confirm)
		case DeleteMessageUponConfirm(channelId, deliveryTag) => deleteMessageUponConfirm(channelId, deliveryTag)
		case RemoveMessagesOlderThan(milliSeconds) => removeMessageOlderThan(milliSeconds)
		case RemoveMultipleConfirmations => removeMultipleConfirmations()
		case LogBufferStatistics(logger) => logBufferStatistics(logger)
	}
}