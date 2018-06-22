package com.kinja.amqp

import java.util.concurrent.TimeUnit

import akka.actor.{ ActorRef, ActorSystem }
import com.github.sstone.amqp.Amqp.{ DeclareExchange, ExchangeParameters, Record, Request }
import com.github.sstone.amqp.{ Amqp, ChannelOwner, ConnectionOwner }

import scala.concurrent.duration.FiniteDuration

class ProducerChannelProvider(
	connection: ActorRef,
	actorSystem: ActorSystem,
	connectionTimeOut: FiniteDuration,
	exchange: ExchangeParameters
) {

	def createChannel(initialCommands: Seq[Request] = Seq.empty[Request]): ActorRef = {
		val initList: Seq[Request] = Seq(Record(DeclareExchange(exchange))) ++ initialCommands

		val channel: ActorRef = ConnectionOwner.createChildActor(
			connection, ChannelOwner.props(init = initList)
		)

		ignore(Amqp.waitForConnection(actorSystem, connection, channel).await(connectionTimeOut.toSeconds, TimeUnit.SECONDS))

		channel
	}
}
