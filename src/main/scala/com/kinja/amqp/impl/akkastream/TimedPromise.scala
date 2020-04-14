package com.kinja.amqp.impl.akkastream

import akka.actor.{ActorSystem, Cancellable}
import com.kinja.amqp.exception.RequestTimedOutException
import com.kinja.amqp.ignore

import scala.concurrent._
import scala.concurrent.duration.FiniteDuration

/**
 * Promise with timeout and handling completing already completed promise gracefully.
 * @param underlying underlying [[scala.concurrent.Promise]]
 * @param cancellable Timeout cancellable.
 * @tparam T type of value of the Promise.
 */
class TimedPromise[T] private (underlying: Promise[T], cancellable: Cancellable) {

  /**
   * Completes the promise with a value if it's not completed already and cancel the timeout.
   * @param v
   */
  def success(v: T): Unit = {
    underlying.synchronized[Unit] {
      if (!underlying.isCompleted) {
        ignore(underlying.success(v))
        ignore(cancellable.cancel())
      }
    }
  }

  /**
   * Completes the promise with a failure if it's not completed already and cancel the timeout.
   * @param cause The cause of the failure.
   */
  def failure(cause: Throwable): Unit = {
    underlying.synchronized[Unit] {
      if (!underlying.isCompleted) {
        ignore(underlying.failure(cause))
        ignore(cancellable.cancel())
      }
    }
  }

  /**
   * Future containing the value of this promise.
   */
  def future: Future[T] = underlying.future

  /**
   * Returns whether the promise has already been completed with a value or an exception.
   */
  def isCompleted: Boolean = underlying.isCompleted

}

object TimedPromise {

  /**
   * Create a promise with timeout.
   * @param actorSystem actor system used for scheduling timeout.
   * @param timeout Timeout.
   * @tparam T type of the value of the promise.
   * @return The created promise.
   */
  def create[T](actorSystem: ActorSystem, timeout: FiniteDuration): TimedPromise[T] = {
    val underlying = Promise.apply[T]()
    val cancellable = actorSystem.scheduler.scheduleOnce(timeout) {
      underlying.synchronized[Unit] {
        if (!underlying.isCompleted) {
          ignore(underlying.failure(new RequestTimedOutException))
        }
      }
    }(actorSystem.dispatcher)
    new TimedPromise(underlying, cancellable)
  }
}