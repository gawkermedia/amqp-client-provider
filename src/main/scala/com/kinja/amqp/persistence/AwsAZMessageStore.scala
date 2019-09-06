package com.kinja.amqp.persistence

/**
  * Interface for marking message stores which persist data only in one AWS availability zone.
  * (Data available only for one cluster in our case.)
  */
trait AwsAZMessageStore extends MessageStore
