package com.kinja.amqp.impl.akkastream

import akka.stream.alpakka.amqp.scaladsl.CommittableReadResult

private[akkastream] sealed trait Acknowledgment

private[akkastream] object Acknowledgment {
	final case class Ack(msg: CommittableReadResult) extends Acknowledgment
	final case class Nack(msg: CommittableReadResult) extends Acknowledgment
}

