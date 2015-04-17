package com.kinja.amqp

import com.kinja.amqp.model.Message
import com.kinja.amqp.model.MessageConfirmation

import org.slf4j.{ Logger => Slf4jLogger }

import akka.actor.ActorSystem
import play.api.libs.json.Json

import java.sql.SQLException

import scala.concurrent.duration._
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.util.Failure
import scala.util.Success
import scala.util.Try

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
	 * @param minMsgAge The minimum age of the message to resend
	 * @param minMultiConfAge The min age of the confirmations for multiple msgs to check for deletion
	 * @param republishTimeout The timeout which we can wait when republishing the msg
	 * @param limit The max number of messages that are processed in each iteration
	 * @param ec Execution context used for scheduling and resend logic
	 */
	def startSchedule(
		initialDelay: FiniteDuration, interval: FiniteDuration, minMsgAge: FiniteDuration, minMultiConfAge: FiniteDuration, republishTimeout: FiniteDuration, limit: Int
	)(implicit ec: ExecutionContext): Unit = {
		actorSystem.scheduler.schedule(initialDelay, interval)(resendUnconfirmed(minMsgAge, minMultiConfAge, republishTimeout, limit))
	}

	private def resendUnconfirmed(minMsgAge: FiniteDuration, minMultiConfAge: FiniteDuration, republishTimeout: FiniteDuration, limit: Int)(implicit ec: ExecutionContext): Unit = {
		producers.foreach {
			case (exchange, producer) =>
				val current = System.currentTimeMillis()
				resendUnconfirmed(current - minMsgAge.toMillis, current - minMultiConfAge.toMillis, republishTimeout, limit, exchange, producer)
		}
	}

	/**
	 * Loads the messages from the store.
	 * For already confirmed messages, deletes the message and the confirmation.
	 * For unconfirmed messages, tries to republish and if succeeds, deletes old message and confirmation.
	 */
	private def resendUnconfirmed(
		msgOlderThan: Long, confOlderThan: Long, republishTimeout: FiniteDuration, limit: Int, exchangeName: String, producer: AmqpProducer
	)(implicit ec: ExecutionContext): Unit = {
		val transactional = messageStore.createTransactionalStore
		transactional.start
		try {
			val oldMessages = transactional.loadMessageOlderThan(msgOlderThan, exchangeName, limit)
			val channels = oldMessages.map(_.channelId).flatten
			val relevantConfirms = transactional.loadConfirmationByChannels(channels)
			val (confirmed, unconfirmed) = oldMessages.partition { msg =>
				relevantConfirms.exists(c => isConfirmedBy(msg, c))
			}

			confirmed.foreach(m => deleteMessageAndMatchingConfirm(m, relevantConfirms, transactional))

			resendAndDelete(unconfirmed, relevantConfirms, producer, transactional, republishTimeout)
		} catch {
			case e: SQLException => logger.warn(s"SQL exception while resending message: $e")
		} finally { transactional.commit }
		try {
			messageStore.deleteMultiConfIfNoMatchingMsg(confOlderThan)
		} catch {
			case e: SQLException => logger.warn(s"SQL exception while deleting multiconfirmations: $e")
		}
	}

	/**
	 * Resend the messages in the list and if managed to publish, delete msg and matching confirmation from the store
	 */
	private def resendAndDelete(
		msgs: List[Message], confs: List[MessageConfirmation], producer: AmqpProducer, transactional: TransactionalMessageStore, republishTimeout: FiniteDuration
	)(implicit ec: ExecutionContext): Unit = {
		msgs.map { msg =>
			val result = Try(Await.result(producer.publish(msg.routingKey, Json.parse(msg.message)), republishTimeout))
			result match {
				case Success(_) => deleteMessageAndMatchingConfirm(msg, confs, transactional)
				case Failure(ex) => logger.warn(s"""Couldn't resend message: $msg, ${ex.getMessage}""")
			}
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

	private def deleteMessageAndMatchingConfirm(msg: Message, confs: List[MessageConfirmation], transactional: TransactionalMessageStore): Unit = {
		transactional.deleteMessage(
			msg.id.getOrElse(throw new IllegalStateException(s"""Fetched message doesn't an have id: $msg"""))
		)
		getMatchingConfirm(msg, confs).foreach { c =>
			transactional.deleteConfirmation(
				c.id.getOrElse(throw new IllegalStateException(s"""Fetched confirmation doesn't an have id: $c"""))
			)
		}
	}

	private def isConfirmedBy(msg: Message, conf: MessageConfirmation): Boolean = {
		msg.channelId == Some(conf.channelId) &&
			(msg.deliveryTag == Some(conf.deliveryTag) || (conf.multiple && msg.deliveryTag.exists(_ <= conf.deliveryTag)))
	}

}
