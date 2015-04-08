package com.kinja.amqp

import com.kinja.amqp.model.Message
import com.kinja.amqp.model.MessageConfirmation

import org.slf4j.{ Logger => Slf4jLogger }

import scala.util.Failure
import scala.util.Success

import scala.concurrent.ExecutionContext.Implicits.global

object UnconfirmedMessageRepeater {

	/**
	 * Loads the messages from the store.
	 * For already confirmed messages, deletes the message and the confirmation.
	 * For unconfirmed messages, tries to republish and if succeeds, deletes old message and confirmation.
	 */
	def resendUnconfirmedMessages(messageStore: MessageStore, olderThan: Long, producer: AmqpProducer, logger: Slf4jLogger): Unit = {
		val oldMessages = messageStore.loadMessageOlderThan(olderThan)
		val channels = oldMessages.map(_.channelId).flatten
		val relevantConfirms = messageStore.loadConfirmationByChannels(channels)
		val (confirmed, unconfirmed) = oldMessages.partition { msg =>
			relevantConfirms.foldRight(false)((conf, confirmed) => confirmed || isConfirmedBy(msg, conf))
		}

		confirmed.foreach(m => deleteMessageAndMatchingConfirm(m, relevantConfirms, messageStore))

		resendAndDelete(unconfirmed, relevantConfirms, producer, messageStore, logger)

	}

	/**
	 * Resend the messages in the list and if managed to publish, delete msg and matching confirmation from the store
	 */
	private def resendAndDelete(msgs: List[Message], confs: List[MessageConfirmation], producer: AmqpProducer, messageStore: MessageStore, logger: Slf4jLogger): Unit = {
		for {
			msg <- msgs
			publishFut = producer.publish(msg.routingKey, msg.message)
			_ = publishFut.map { result =>
				result match {
					case Success(_) => deleteMessageAndMatchingConfirm(msg, confs, messageStore)
					case Failure(ex) => logger.warn(s"""Couldn't resend message: $msg, ${ex.getMessage}""")
				}
			}
		} yield Unit
	}

	private def getMatchingConfirm(msg: Message, confs: List[MessageConfirmation]): Option[MessageConfirmation] = {
		for {
			channelId <- msg.channelId
			deliveryTag <- msg.deliveryTag
			relevantConfirm <- confs.find(c => (!c.multiple && c.channelId == channelId && c.deliveryTag == deliveryTag))
		} yield {
			relevantConfirm
		}
	}

	private def deleteMessageAndMatchingConfirm(msg: Message, confs: List[MessageConfirmation], messageStore: MessageStore): Unit = {
		messageStore.deleteMessage(msg.id.getOrElse(throw new IllegalStateException(s"""Fetched message doesn't an have id: $msg""")))
		getMatchingConfirm(msg, confs).map { c =>
			messageStore.deleteConfirmation(c.id.getOrElse(throw new IllegalStateException(s"""Fetched confirmation doesn't an have id: $c""")))
		}
	}

	private def isConfirmedBy(msg: Message, conf: MessageConfirmation): Boolean = {
		msg.channelId == Some(conf.channelId) &&
			(msg.deliveryTag == Some(conf.deliveryTag) || (conf.multiple && msg.deliveryTag.map(_ <= conf.deliveryTag).getOrElse(false)))
	}

}
