package com.kinja.amqp

trait AmqpClientInterface extends AmqpConsumerClientInterface {

	def getMessageProducer(exchangeName: String): AmqpProducerInterface

}
