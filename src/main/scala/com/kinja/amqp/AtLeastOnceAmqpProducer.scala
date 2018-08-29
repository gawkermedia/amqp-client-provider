package com.kinja.amqp

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import com.github.sstone.amqp.Amqp._
import com.github.sstone.amqp.{ Amqp, ChannelOwner, ConnectionOwner }
import com.kinja.amqp.model.{ Message, MessageConfirmation }
import com.kinja.amqp.persistence.MessageStore
import com.rabbitmq.client.AMQP.BasicProperties
import org.slf4j.{ Logger => Slf4jLogger }

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }

class AtLeastOnceAmqpProducer(
	exchangeName: String,
	channelProvider: ProducerChannelProvider,
	actorSystem: ActorSystem,
	messageStore: MessageStore,
	askTimeout: FiniteDuration,
	logger: Slf4jLogger
)(implicit val ec: ExecutionContext) extends AmqpProducerInterface {

	private implicit val timeout: Timeout = Timeout(askTimeout)

	private val initialCommands = Seq[Request](
		ConfirmSelect,
		AddConfirmListener(createConfirmListener)
	)
	private val channel: ActorRef = channelProvider.createChannel(initialCommands)

	def publish[A: Writes](
		routingKey: String,
		message: A,
		saveTimeMillis: Long = System.currentTimeMillis()
	): Future[Unit] = {
		val messageString = implicitly[Writes[A]].writes(message)
		val bytes = messageString.getBytes(java.nio.charset.Charset.forName("UTF-8"))
		val properties = new BasicProperties.Builder().deliveryMode(2).build()
		channel ? Publish(
			exchangeName, routingKey, bytes, properties = Some(properties), mandatory = true, immediate = false
		) map {
			case Ok(_, Some(MessageUniqueKey(deliveryTag, channelId))) =>
				messageStore.saveMessages(
					List(Message(
						None,
						routingKey,
						exchangeName,
						messageString,
						Some(channelId),
						Some(deliveryTag),
						new Timestamp(saveTimeMillis)
					))
				)
			case _ =>
				messageStore.saveMessages(
					List(Message(
						None,
						routingKey,
						exchangeName,
						messageString,
						None,
						None,
						new Timestamp(saveTimeMillis)
					))
				)
		} recoverWith {
			case _ => Future(
				messageStore.saveMessages(
					List(Message(
						None,
						routingKey,
						exchangeName,
						messageString,
						None,
						None,
						new Timestamp(saveTimeMillis)
					))
				)
			)
		}
	}

	private def handleConfirmation(
		channelId: String, deliveryTag: Long, multiple: Boolean, timestamp: Long
	): Unit = {
		ignore(Future {
			if (multiple) {
				logger.debug("[RabbitMQ] Got multiple confirmation, saving...")
				messageStore.saveConfirmations(
					List(MessageConfirmation(
						None,
						channelId,
						deliveryTag,
						multiple,
						new Timestamp(timestamp)
					))
				)
			} else {
				messageStore.deleteMessageUponConfirm(channelId, deliveryTag).map {
					case true =>
						logger.debug("[RabbitMQ] Message deleted upon confirm, no need to save confirmation")
					case _ =>
						logger.debug("[RabbitMQ] Message wasn't deleted upon confirm, saving confirmation")
						messageStore.saveConfirmations(
							List(MessageConfirmation(
								None,
								channelId,
								deliveryTag,
								multiple,
								new Timestamp(timestamp)
							))
						)
				}
			}
		})
	}

	private def createConfirmListener: ActorRef = actorSystem.actorOf(Props(new Actor {
		def receive = {
			case HandleAck(deliveryTag, multiple, channelId, timestamp) =>
				handleConfirmation(channelId, deliveryTag, multiple, timestamp)
			case HandleNack(deliveryTag, multiple, channelId, timestamp) =>
				logger.warn(
					s"""[RabbitMQ] Receiving HandleNack with delivery tag: $deliveryTag,
					 | multiple: $multiple, channelId: $channelId"""
				)
		}
	}))
}
