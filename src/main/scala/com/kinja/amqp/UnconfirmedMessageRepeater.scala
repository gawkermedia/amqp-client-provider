package com.kinja.amqp

import com.kinja.amqp.model.Message
import com.kinja.amqp.model.MessageConfirmation

import org.slf4j.{ Logger => Slf4jLogger }

import akka.actor.ActorSystem

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import scala.util.Failure
import scala.util.Success

class UnconfirmedMessageRepeater(
	actorSystem: ActorSystem,
	messageStore: MessageStore,
	producers: Map[String, AmqpProducer],
	logger: Slf4jLogger
) {

	/**
	 * Schedules message resend logic periodically
	 * @param initialDelay The delay to start scheduling after
	 * @param interval Interval between two scheduled actions
	 * @param minAge The minimum age of the message to resend
	 * @param limit The max number of messages that are processed in each iteration
	 * @param ec Execution context used for scheduling and resend logic
	 */
	def startSchedule(
		initialDelay: FiniteDuration, interval: FiniteDuration, minAge: FiniteDuration, limit: Int
	)(implicit ec: ExecutionContext): Unit = {
		actorSystem.scheduler.schedule(initialDelay, interval)(resendUnconfirmed(minAge, limit))
	}

	private def resendUnconfirmed(minAge: FiniteDuration, limit: Int)(implicit ec: ExecutionContext): Unit = {
		producers.foreach {
			case (exchange, producer) =>
				resendUnconfirmed(System.currentTimeMillis() - minAge.toMillis, limit, exchange, producer)
		}
	}

	/**
	 * Loads the messages from the store.
	 * For already confirmed messages, deletes the message and the confirmation.
	 * For unconfirmed messages, tries to republish and if succeeds, deletes old message and confirmation.
	 */
	private def resendUnconfirmed(
		olderThan: Long, limit: Int, exchangeName: String, producer: AmqpProducer
	)(implicit ec: ExecutionContext): Unit = {
		messageStore.withLockingTransaction {
			val oldMessages = messageStore.loadMessageOlderThan(olderThan, exchangeName, limit)
			val channels = oldMessages.map(_.channelId).flatten
			val relevantConfirms = messageStore.loadConfirmationByChannels(channels)
			val (confirmed, unconfirmed) = oldMessages.partition { msg =>
				relevantConfirms.exists(c => isConfirmedBy(msg, c))
			}

			confirmed.foreach(m => deleteMessageAndMatchingConfirm(m, relevantConfirms))

			resendAndDelete(unconfirmed, relevantConfirms, producer)
		}
	}

	/**
	 * Resend the messages in the list and if managed to publish, delete msg and matching confirmation from the store
	 */
	private def resendAndDelete(
		msgs: List[Message], confs: List[MessageConfirmation], producer: AmqpProducer
	)(implicit ec: ExecutionContext): Unit = {
		for {
			msg <- msgs
			publishFut = producer.publish(msg.routingKey, msg.message)
		} yield publishFut.onComplete {
			case Success(_) => deleteMessageAndMatchingConfirm(msg, confs)
			case Failure(ex) => logger.warn(s"""Couldn't resend message: $msg, ${ex.getMessage}""")
		}
	}

	private def getMatchingConfirm(msg: Message, confs: List[MessageConfirmation]): Option[MessageConfirmation] = {
		for {
			channelId <- msg.channelId
			deliveryTag <- msg.deliveryTag
			relevantConfirm <- confs.find(c => !c.multiple && c.channelId == channelId && c.deliveryTag == deliveryTag)
		} yield {
			relevantConfirm
		}
	}

	private def deleteMessageAndMatchingConfirm(msg: Message, confs: List[MessageConfirmation]): Unit = {
		messageStore.deleteMessage(
			msg.id.getOrElse(throw new IllegalStateException(s"""Fetched message doesn't an have id: $msg"""))
		)
		getMatchingConfirm(msg, confs).foreach { c =>
			messageStore.deleteConfirmation(
				c.id.getOrElse(throw new IllegalStateException(s"""Fetched confirmation doesn't an have id: $c"""))
			)
		}
	}

	private def isConfirmedBy(msg: Message, conf: MessageConfirmation): Boolean = {
		msg.channelId == Some(conf.channelId) &&
			(msg.deliveryTag == Some(conf.deliveryTag) || (conf.multiple && msg.deliveryTag.exists(_ <= conf.deliveryTag)))
	}

}
