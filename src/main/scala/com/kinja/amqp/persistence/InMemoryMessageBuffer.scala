package com.kinja.amqp.persistence

import java.sql.Timestamp

import akka.actor.{ Actor, ActorLogging }
import akka.event.LoggingReceive
import com.kinja.amqp.ignore
import com.kinja.amqp.model.{ Message, MessageConfirmation }
import org.slf4j.{ Logger => Slf4jLogger }

import scala.collection.mutable.{ ArrayBuffer, Map => MutableMap }

final case class SaveMessages(messages: List[Message])
final case class MultipleConfirmations(confirms: List[MessageConfirmation])
final case class DeleteMessageUponConfirm(channelId: String, deliveryTag: Long)
final case class RemoveMessagesOlderThan(milliSeconds: Long)
case object GetAllMessages
case object RemoveMultipleConfirmations
final case class LogBufferStatistics(logger: Slf4jLogger)

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

	private def saveMessage(message: Message): Unit = ignore(messageBuffer += message)

	private def saveMessages(messages: List[Message]): Unit = ignore(messageBuffer ++= messages)

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

		messagesToRemove.foreach { message =>
			messageBuffer -= message
		}

		sender ! messagesToRemove
	}

	private def removeMultipleConfirmations(): Unit = {
		val confirmationList: List[MessageConfirmation] = confirmations.toList.map {
			case (channelId: String, deliveryTag: Long) =>
				MessageConfirmation(
					None, channelId, deliveryTag, multiple = true, new Timestamp(System.currentTimeMillis())
				)
		}

		sender ! confirmationList

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

	def getAllMessages(): Unit = {
		sender ! messageBuffer.toList
	}

	override def receive: Receive = LoggingReceive {
		case SaveMessages(messages) => saveMessages(messages)
		case MultipleConfirmations(confirms) => confirms.foreach(handleMultipleConfirmation)
		case DeleteMessageUponConfirm(channelId, deliveryTag) => deleteMessageUponConfirm(channelId, deliveryTag)
		case RemoveMessagesOlderThan(milliSeconds) => removeMessageOlderThan(milliSeconds)
		case GetAllMessages => getAllMessages()
		case RemoveMultipleConfirmations => removeMultipleConfirmations()
		case LogBufferStatistics(logger) => logBufferStatistics(logger)
	}
}
