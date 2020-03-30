package com.kinja.amqp

import akka.Done

import scala.concurrent.Future

trait WithShutdown {

  def shutdown: Future[Done]
}
