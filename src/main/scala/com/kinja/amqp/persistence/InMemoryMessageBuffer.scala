package com.kinja.amqp.persistence

import java.sql.Timestamp

import akka.actor.{ Actor, ActorLogging }
import akka.event.LoggingReceive
import com.kinja.amqp.ignore
import com.kinja.amqp.model.{ FailedMessage, Message, MessageConfirmation, MessageLike }
import org.slf4j.{ Logger => Slf4jLogger }

import scala.collection.mutable.{ ArrayBuffer, Map => MutableMap }

final case class SaveMessages(messages: List[MessageLike])
final case class MultipleConfirmations(confirms: List[MessageConfirmation])
final case class DeleteMessageUponConfirm(channelId: String, deliveryTag: Long)
final case class RemoveMessagesOlderThan(milliSeconds: Long)
case object GetAllMessages
case object RemoveMultipleConfirmations
final case class LogBufferStatistics(logger: Slf4jLogger)

@SuppressWarnings(Array("org.wartremover.warts.StringPlusAny"))
class InMemoryMessageBuffer extends Actor with ActorLogging {

	@SuppressWarnings(Array("org.wartremover.warts.MutableDataStructures"))
	val messageBuffer: ArrayBuffer[MessageLike] = ArrayBuffer()

	@SuppressWarnings(Array("org.wartremover.warts.MutableDataStructures"))
	val confirmations: MutableMap[String, Long] = MutableMap()

	private def handleMultipleConfirmation(confirm: MessageConfirmation): Unit = {
		if (!confirm.multiple) {
			throw new IllegalStateException("Got single confirm instead of multiple")
		}

		val messagesToDelete: ArrayBuffer[MessageLike] = messageBuffer.filter {
			case Message(_, _, _, channelId, deliveryTag, _) =>
				channelId == confirm.channelId && deliveryTag < confirm.deliveryTag
			case FailedMessage(_, _, _, _, _) => false
		}

		messagesToDelete.foreach { message =>
			messageBuffer -= message
		}

		confirmations.update(confirm.channelId, confirm.deliveryTag)
	}

	private def saveMessages(messages: List[MessageLike]): Unit = ignore(messageBuffer ++= messages)

	private def deleteMessageUponConfirm(channelId: String, deliveryTag: Long): Unit = {
		val messageToDelete: Option[MessageLike] = messageBuffer.find {
			case Message(_, _, _, mChannelId, mDeliveryTag, _) =>
				mChannelId == channelId && mDeliveryTag == deliveryTag
			case FailedMessage(_, _, _, _, _) => false
		}

		messageToDelete.foreach { message =>
			messageBuffer -= message
		}

		sender() ! messageToDelete.isDefined
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

		sender() ! messagesToRemove
	}

	private def removeMultipleConfirmations(): Unit = {
		val confirmationList: List[MessageConfirmation] = confirmations.toList.map {
			case (channelId: String, deliveryTag: Long) =>
				MessageConfirmation(
					channelId, deliveryTag, multiple = true, new Timestamp(System.currentTimeMillis())
				)
		}

		sender() ! confirmationList

		confirmations.clear()
	}

	private def logBufferStatistics(logger: Slf4jLogger): Unit = {
		val time = System.currentTimeMillis()
		val averageAge = if (messageBuffer.nonEmpty) {
			val time = System.currentTimeMillis()
			val allAge = messageBuffer.foldLeft(0L) { (acc: Long, message: MessageLike) =>
				acc + (time - message.createdTime.getTime)
			}
			allAge / messageBuffer.size
		} else {
			0
		}

		val maxAge = messageBuffer.foldLeft(0L) { (acc: Long, message: MessageLike) =>
			val age = time - message.createdTime.getTime
			Math.max(acc, age)
		}
		if (messageBuffer.nonEmpty) {
			logger.info(s"There are ${messageBuffer.size} message in the buffer, " +
				s"with average age of $averageAge millis, " +
				s"max age is $maxAge millis")
		}
		if (confirmations.nonEmpty) {
			logger.info(s"There are ${confirmations.size} confirmations in the buffer, and they are $confirmations")
		}
	}

	def getAllMessages(): Unit = {
		sender() ! messageBuffer.toList
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
