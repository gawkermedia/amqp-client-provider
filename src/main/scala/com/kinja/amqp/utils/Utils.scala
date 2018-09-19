package com.kinja.amqp.utils

import java.util.concurrent.TimeoutException

import akka.actor.ActorSystem
import akka.pattern.after

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

object Utils {

	/**
	 * Calls a future with timeout
	 * @param name descriptive name of the step (this became part of the TimeoutExceptions message in case of timeout)
	 * @param step step to execute with timeout boundary
	 * @param timeout timeout boundary
	 * @param actorSystem
	 * @tparam T return type of steps Future
	 * @return result or failed future with TimeOutException
	 */
	def withTimeout[T](name: String, step: => Future[T], timeout: FiniteDuration)(actorSystem: ActorSystem): Future[T] = {
		val timeoutF = after(timeout, actorSystem.scheduler)(Future.failed[T](new TimeoutException(s"$name timed out after $timeout")))(actorSystem.dispatcher)
		Future.firstCompletedOf(List(step, timeoutF))(actorSystem.dispatcher)
	}

}
