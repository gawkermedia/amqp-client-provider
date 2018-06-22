package com.kinja.amqp

import com.rabbitmq.client.Address
import com.github.sstone.amqp.Amqp._
import com.typesafe.config.Config
import com.typesafe.config.ConfigException.{ BadValue, Missing }

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.Try
import scala.util.control.NonFatal

final case class ResendLoopConfig(
	republishTimeoutInSec: FiniteDuration,
	initialDelayInSec: FiniteDuration,
	bufferProcessInterval: FiniteDuration,
	messageBatchSize: Int,
	memoryFlushInterval: FiniteDuration,
	memoryFlushChunkSize: Int,
	memoryFlushTimeOut: FiniteDuration
)

final case class AtLeastOnceGroup(name: String)

object AtLeastOnceGroup {

	val default: AtLeastOnceGroup = AtLeastOnceGroup("default")

}

final case class ProducerConfig(atLeastOnceGroup: AtLeastOnceGroup, exchangeParams: ExchangeParameters)

trait AmqpConfiguration {
	protected val config: Config

	val username: String = config.getString("messageQueue.username")
	val password: String = config.getString("messageQueue.password")
	val heartbeatRate: Int = config.getInt("messageQueue.heartbeatRate")
	val connectionTimeOut: FiniteDuration = config.getLong("messageQueue.connectionTimeoutInSec").seconds
	val askTimeOut: FiniteDuration = config.getLong("messageQueue.askTimeoutInMilliSec").millis
	val testMode: Boolean = Try(config.getBoolean("messageQueue.testMode")).getOrElse(false)

	private val hosts: Seq[String] = config.getStringList("messageQueue.hosts").asScala.toSeq

	val addresses: Array[Address] = scala.util.Random.shuffle(hosts.map(new Address(_))).toArray

	val exchanges: Map[String, ProducerConfig] = createExchangeParamsForAll()

	val queues: Map[String, QueueWithRelatedParameters] = createQueueParamsForAll()

	val resendConfig: Option[ResendLoopConfig] = loadResendConfig()

	private def loadResendConfig(): Option[ResendLoopConfig] = {
		try {
			def withDefault[T](value: => T, default: => T): T =
				try {
					value
				} catch {
					case _: Missing => default
				}
			val republishTimeout = withDefault(config.getLong("messageQueue.resendLoop.republishTimeoutInSec"), 10).seconds
			val initialDelay = withDefault(config.getLong("messageQueue.resendLoop.initialDelayInSec"), 2).seconds
			val bufferProcessInterval = withDefault(config.getLong("messageQueue.resendLoop.bufferProcessIntervalInSec"), 5).seconds
			val messageBatchSize = withDefault(config.getInt("messageQueue.resendLoop.messageBatchSize"), 30)
			val memoryFlushInterval = withDefault(config.getLong("messageQueue.resendLoop.memoryFlushIntervalInMilliSec"), 3000).milliseconds
			val memoryFlushChunkSize = withDefault(config.getInt("messageQueue.resendLoop.memoryFlushChunkSize"), 200)
			val memoryFlushTimeOut = withDefault(config.getLong("messageQueue.resendLoop.memoryFlushTimeOutInSec"), 10).seconds

			Some(
				ResendLoopConfig(
					republishTimeout,
					initialDelay,
					bufferProcessInterval,
					messageBatchSize,
					memoryFlushInterval,
					memoryFlushChunkSize,
					memoryFlushTimeOut
				)
			)
		} catch {
			case NonFatal(e) => None
		}
	}

	private def createExchangeParamsForAll(): Map[String, ProducerConfig] = {
		val builtinAtLeastOnceGroup: AtLeastOnceGroup =
			if (config.hasPath("messageQueue.builtinAtLeastOnceGroup")) {
				AtLeastOnceGroup(config.getString("messageQueue.builtinAtLeastOnceGroup"))
			} else {
				AtLeastOnceGroup.default
			}
		val names: Set[String] = config.getConfig("messageQueue.exchanges").root().keySet().asScala.toSet

		names.map { name =>
			name -> createExchangeParams(name)
		}.toMap ++ getBuiltInExchangeParams(builtinAtLeastOnceGroup)
	}

	private def createQueueParamsForAll(): Map[String, QueueWithRelatedParameters] = {
		val names: Set[String] = config.getConfig("messageQueue.queues").root().keySet().asScala.toSet

		names.map { name =>
			val queueConfig: Config = config.getConfig(s"messageQueue.queues.$name")

			val boundExchangeName = queueConfig.getString("exchange")

			val boundExchange: ProducerConfig = exchanges.getOrElse(
				boundExchangeName, throw new Missing(s"messageQueue.exchanges.$boundExchangeName")
			)

			val routingKey = queueConfig.getString("routingKey")

			val deadLetterExchangeName: Option[String] = if (queueConfig.hasPath("deadLetterExchange")) {
				Some(queueConfig.getString("deadLetterExchange"))
			} else {
				None
			}

			val deadLetterExchangeParams: Option[ExchangeParameters] = deadLetterExchangeName.map(
				exchanges.getOrElse(_, throw new Missing(s"messageQueue.queues.$name.exchange")).exchangeParams
			)

			val additionalParams: Map[String, String] = deadLetterExchangeName
				.map(name => Map("x-dead-letter-exchange" -> name)).getOrElse(Map.empty[String, String])

			val queueParameters = QueueParameters(
				name, passive = false, durable = true, exclusive = false, autodelete = false, additionalParams
			)

			name -> QueueWithRelatedParameters(
				queueParameters, boundExchange.exchangeParams, deadLetterExchangeParams, routingKey
			)
		}.toMap
	}

	private def createExchangeParams(name: String): ProducerConfig = {
		val exchangeConfig: Config = config.getConfig(s"messageQueue.exchanges.$name")

		val exchangeType = if (exchangeConfig.hasPath("type")) {
			exchangeConfig.getString("type")
		} else {
			"direct"
		}

		val atLeastOnceGroup: AtLeastOnceGroup = if (exchangeConfig.hasPath("atLeastOnceGroup")) {
			AtLeastOnceGroup(exchangeConfig.getString("atLeastOnceGroup"))
		} else {
			AtLeastOnceGroup.default
		}

		val extraParams: Map[String, AnyRef] = if (exchangeConfig.hasPath("extraParams")) {
			exchangeConfig.getConfig("extraParams").root().unwrapped().asScala.toMap
		} else {
			Map.empty[String, AnyRef]
		}

		ProducerConfig(atLeastOnceGroup, ExchangeParameters(name, passive = false, exchangeType, durable = true, autodelete = false, extraParams))
	}

	private def getBuiltInExchangeParams(builtinAtLeastOnceGroup: AtLeastOnceGroup): Map[String, ProducerConfig] = {
		Map(
			"amq.topic" -> ProducerConfig(builtinAtLeastOnceGroup, StandardExchanges.amqTopic),
			"amq.direct" -> ProducerConfig(builtinAtLeastOnceGroup, StandardExchanges.amqDirect),
			"amq.fanout" -> ProducerConfig(builtinAtLeastOnceGroup, StandardExchanges.amqFanout),
			"amq.headers" -> ProducerConfig(builtinAtLeastOnceGroup, StandardExchanges.amqHeaders),
			"amq.match" -> ProducerConfig(builtinAtLeastOnceGroup, StandardExchanges.amqMatch)
		)
	}
}
