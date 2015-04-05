package com.kinja.amqp

import com.rabbitmq.client.Address

import com.github.sstone.amqp.Amqp._

import com.typesafe.config.Config
import com.typesafe.config.ConfigException.Missing

import scala.collection.JavaConverters._

trait AmqpConfiguration {
	protected val config: Config

	val username = config.getString("messageQueue.username")
	val password = config.getString("messageQueue.password")
	val heartbeatRate = config.getInt("messageQueue.heartbeatRate")
	val connectionTimeOut = config.getLong("messageQueue.connectionTimeoutInSec")

	private val hosts: Seq[String] = config.getStringList("messageQueue.hosts").asScala.toSeq

	val addresses: Array[Address] = scala.util.Random.shuffle(hosts.map(new Address(_))).toArray

	private val exchanges: Map[String, ExchangeParameters] = createExchangeParamsForAll()

	private val queues: Map[String, QueueWithRelatedParameters] = createQueueParamsForAll()

	def getExchangeParams(name: String): ExchangeParameters = {
		exchanges.getOrElse(name, throw new Missing(s"No such exchange with name $name"))
	}

	def getQueueParams(name: String): QueueWithRelatedParameters = {
		queues.getOrElse(name, throw new Missing(s"No such queue with name $name"))
	}

	private def createExchangeParamsForAll(): Map[String, ExchangeParameters] = {
		val names: Set[String] = config.getConfig("messageQueue.exchanges").root().keySet().asScala.toSet

		names.map { name =>
			name -> createExchangeParams(name)
		}.toMap ++ getBuiltInExchangeParams
	}

	private def createQueueParamsForAll(): Map[String, QueueWithRelatedParameters] = {
		val names: Set[String] = config.getConfig("messageQueue.queues").root().keySet().asScala.toSet

		names.map { name =>
			val queueConfig: Config = config.getConfig(s"messageQueue.queues.$name")

			val boundExchangeName = queueConfig.getString("exchange")

			val boundExchangeParams: ExchangeParameters = exchanges.getOrElse(
				boundExchangeName, throw new Missing(s"messageQueue.exchanges.$boundExchangeName"))

			val routingKey = queueConfig.getString("routingKey")

			val deadLetterExchangeName: Option[String] = if (queueConfig.hasPath("deadLetterExchange")) {
				Some(queueConfig.getString("deadLetterExchange"))
			} else {
				None
			}

			val deadLetterExchangeParams: Option[ExchangeParameters] = deadLetterExchangeName.map(
				exchanges.getOrElse(_, throw new Missing(s"messageQueue.queues.$name.exchange")))

			val additionalParams: Map[String, String] = deadLetterExchangeName
				.map(name => Map("x-dead-letter-exchange" -> name)).getOrElse(Map.empty)

			val queueParameters = QueueParameters(
				name, passive = false, durable = true, exclusive = false, autodelete = false, additionalParams)

			name -> QueueWithRelatedParameters(queueParameters, boundExchangeParams, deadLetterExchangeParams, routingKey)
		}.toMap
	}

	private def createExchangeParams(name: String): ExchangeParameters = {
		val exchangeConfig: Config = config.getConfig(s"messageQueue.exchanges.$name")

		val exchangeType = if (exchangeConfig.hasPath("type")) {
			exchangeConfig.getString("type")
		} else {
			"direct"
		}

		val extraParams: Map[String, AnyRef] = if (exchangeConfig.hasPath("extraParams")) {
			exchangeConfig.getConfig("extraParams").root().unwrapped().asScala.toMap
		} else {
			Map.empty[String, AnyRef]
		}

		ExchangeParameters(name, passive = false, exchangeType, durable = true, autodelete = false, extraParams)
	}

	private def getBuiltInExchangeParams: Map[String, ExchangeParameters] = {
		Map(
			"amq.topic" -> StandardExchanges.amqTopic,
			"amq.direct" -> StandardExchanges.amqDirect,
			"amq.fanout" -> StandardExchanges.amqFanout,
			"amq.headers" -> StandardExchanges.amqHeaders,
			"amq.match" -> StandardExchanges.amqMatch)
	}
}