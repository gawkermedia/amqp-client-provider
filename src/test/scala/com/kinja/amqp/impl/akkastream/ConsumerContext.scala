package com.kinja.amqp.impl.akkastream

import akka.actor.ActorSystem
import akka.stream.Materializer
import org.specs2.execute.{ AsResult, Result }
import org.specs2.specification.{ Before, ForEach }

import scala.concurrent.{ Await, ExecutionContext }
import scala.concurrent.duration._
import scala.util.Try

trait ConsumerContext extends ForEach[ConsumerTestFactory] with Before {

	implicit val system: ActorSystem = ActorSystem("test")
	implicit val materializer: Materializer = Materializer(system)
	implicit val ec: ExecutionContext = system.dispatcher

	override def before: Any = {
		//Clearing the unexpected left overs of the previous test runs
		val factory = new ConsumerTestFactory
		factory.deleteBindingQueueAndExchange(factory.defaultQueueWithRelatedParameters)
		Await.result(factory.shutdown, 2.seconds)
	}

	override def foreach[R: AsResult](f: ConsumerTestFactory => R): Result = {
		val factory = new ConsumerTestFactory
		val s = Try(AsResult(f(factory)))
		Await.result(factory.shutdown, 10.seconds)
		s.get
	}
}
