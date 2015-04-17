package com.kinja.amqp.exception

class MissingProducerException(name: String) extends Exception(s"""There is no producer for exchange with name "$name", please check your configuration.""")

class MissingConsumerException(name: String) extends Exception(s"""There is no consumer for queue with name "$name", please check your configuration.""")

class MissingResendConfigException extends Exception("Resend loop configuration is missing")
