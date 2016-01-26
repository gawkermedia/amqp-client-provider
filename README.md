# What is this?

This is an abstract layer between https://github.com/gawkermedia/amqp-client and your application.

# What does it do?

Provides at least once guarantee on message delivery and the ease of configuring producers/consumers.

# How does it do it?

### Handles publisher confirmations and message republishing if messages weren't confirmed

With a configurable `MessageStore`, every message sent to RabbitMQ (we send messages with persistent flag by default) gets saved to the `MessageStore` (currently, you can choose between a NOOP and MySql backed implementation).
Then if the RabbitMQ sends back a confirmation that the message was persisted on the broker side, the message gets deleted. If no confirmation arrived within the configured timeframe, the `Repeater` resends with the same `Publisher`. The message will be picked up by the resend loop until it finally gets confirmed.

### Automatically sends consumer confirmations after the message was processed

The only thing you need to provide is a simple configuration and a function of `A => Unit`, and the library will handle the rest: creates the queue and the exchange if they does not exist, creates the binding, and starts consuming messages. With every message, your function gets called. After processing the message, the library will send back the acknowledgement to RabbitMQ.
 If your code throws exception, the library sends back a negative acknowledgement and the message will be requeued. Right now if you have Futures within your function, you must Await it in order to have to ability to requeue the message in case of any error. It will be changed in the future and you'll have to provide an `A => Future[Unit]` type of function which will be awaited by the library, so you can't forget it.
   

### Configuration

You can configure host names, username, password, heartbeat rate, connection timeouts, resender frequencies, exchanges, queues and bindings between them.

This is a sample configuration with everything that configurable:

```
messageQueue {
	hosts = [
		"rabbit1.your.domain.com",
		"rabbit2.your.domain.com"
	]
	username = "guest"
	password = "guest"
	connectionTimeoutInSec = 10
	heartbeatRate = 60
	askTimeoutInMilliSec = 100
	exchanges {
		your-events {
			type = "topic"
		},
	},
	queues {
		your-updates {
			exchange = "your-events"
			routingKey = "whatever.updated"
		},
		test-messages {
			exchange = "amq.topic"
			routingKey = "test.messages"
		}
	}
	resendLoop {
		republishTimeoutInSec = 10
		initialDelayInSec = 2
		bufferProcessIntervalInSec = 5
		minMsgAgeInSec = 5
		maxMultiConfAgeInSec = 30
		maxSingleConfAgeInSec = 30
		messageBatchSize = 30
		messageLockTimeOutAfterSec = 60
		memoryFlushIntervalInMilliSec = 3000
		memoryFlushChunkSize = 200
		memoryFlushTimeOutInSec = 10
	}
}
```

So your options are:

* `hosts`: An array of host names in the cluster. If you have only one server, it will be a single-element array.
* `username` and `password`: What they look like.
* `connectionTimeoutInSec`: Number of seconds to Await on trying to obtain connection.
* `heartbeatRate`: Number of seconds for RabbitMQ [requested heartbeat](http://www.rabbitmq.com/heartbeats.html).
* `askTimeoutInMilliSec`: Number of milliseconds to await an Akka response on publishing a message. This is used when we want to store the delivery tag of the message to handle confirmations and resending messages which lack confirmation. 
* `exchanges`: The list of exchanges you would like to use. Built in exchanges (amq.direct, amq.topic, etc.) are included by default, you don't have to add them here. The index of the exchange config will be the name of the exchange. With every exchange, you can configure:
  * `type`: The type of exchange (direct, topic, fanout, headers) 
  * `deadLetterExchange`: The name of dead letter exchange for the exchange. You have to configure that here also, or you can use on of the built in exchanges. 
* `queues`: The list of queues you want to consume messages from. You can declare the queue's name (the index of the queue configuration), the exchange you want to bind the queue to, and the binding key for the binging. Your options of configuration are:
  * `exchange`: The exchange name to bind to queue to. It must exist in the `exchanges` above or be one of the built in exchange, list amq.topic
  * `routingKey`: The routing key for the binding.
* `resendLoop`: To ensure at-least-once-delivery and that the application is functional while RabbitMQ isn't functional, there's a message buffer where unconfirmed messages are stored until we get back confirmation from RabbitMQ. TODO more about this link to documentaion on that. TODO: set these values as default, so there's no need for all of these values in config as this works fine.
  * `republishTimeoutInSec`: Timeout in seconds for republishing an unconfirmed message. Set it higher then `askTimeOutInMilliSec`. TODO: think about getting rid of it and use a value based on `askTimeOutInMilliSec`
  * `initialDelayInSec`: The number of seconds to wait before starting processing the message buffer
  * `bufferProcessIntervalInSec`: The number of seconds between processing message buffer
  * `minMsgAgeInSec`: The minimum age of an unconfirmed message which gets resent to RabbitMQ. This is the interval we wait for RabbitMQ to confirm a message, after we consider it unconfirmed (lost), and we resend it.
  * `maxMultiConfAgeInSec`: The number of seconds before a confirmation which confirmed multiple messages gets deleted. See [RabbitMQ documentation on confirms](https://www.rabbitmq.com/confirms.html)
  * `maxSingleConfAgeInSec`: The number of seconds before a confirmation which confirmed a single message gets deleted.
  * `messageBatchSize`: The number of messages to resend in one batch.
  * `messageLockTimeOutAfterSec`: The number of seconds after locked messages by a previous batch considered timed out, and this way gets resent.
  * `memoryFlushIntervalInMilliSec`: There's an in-memory buffer on top of the MySQL backed message buffer. This is the interval which after the messages got flushed to MySQL after they were published.
  * `memoryFlushChunkSize`: The number of messages got flushed to MySQL in one batch.
  * `memoryFlushTimeOutInSec`: The timeout of one batch of flush.
  
# How do I use it?

### Configuration

First, you will need an actual configuration. Let's just have a singleton one, we don't need new instances of that.

```scala
import com.kinja.amqp.AmqpConfiguration
import com.typesafe.config.Config

object ProductionAmqpConfiguration extends AmqpConfiguration {
	protected override lazy val config: Config = com.typesafe.config.ConfigFactory.load
}
```

### A connection

Let's have a singleton object of the connection also. In this example we will use Play Framework's default actorsystem.

```scala
import com.rabbitmq.client.ConnectionFactory
import com.github.sstone.amqp.ConnectionOwner
import scala.concurrent.duration._

object ProductionAmqpConnection {
	val actorSystem = play.libs.Akka.system

	val factory = new ConnectionFactory()
	factory.setUsername(ProductionAmqpConfiguration.username)
	factory.setPassword(ProductionAmqpConfiguration.password)
	factory.setRequestedHeartbeat(ProductionAmqpConfiguration.heartbeatRate)

	val connection = actorSystem.actorOf(
		ConnectionOwner.props(
			factory,
			ProductionAmqpConfiguration.connectionTimeOut.seconds,
			addresses = Some(ProductionAmqpConfiguration.addresses)))
}
```

### A client registry

This will hold an producer/consumer for each exchange/queue you declared (including the default built in exchanges).
In the following example we will use Play Framework's default actorsystem and create a new slf4j logger. The `RabbitMQNullMessageStore` is just a discarding messagestore which does nothing. You might want to use your in-memory or Redis backed store or you can go with the built in MySql backed implementation called `MySqlMessageStore`.

```scala
import org.slf4j.{ Logger => Slf4jLogger, LoggerFactory }
import com.kinja.common.akka.ActorSystem

object ProductionAmqpClientRegistry
	extends AmqpClientRegistry {
	
	protected override lazy val connection = ProductionAmqpConnection.connection
    
	protected override lazy val configuration: AmqpConfiguration = ProductionAmqpConfiguration

	protected override lazy val messageStore: MessageStore = RabbitMQNullMessageStore

	override lazy val actorSystem: ActorSystem = play.libs.Akka.system

	override protected lazy val logger: Logger = LoggerFactory.getLogger(this.getClass().getName())
}
```

### The client provider you can mix in or initiate as a separate instance

This will be the one you'll mix in into your code if you use cake pattern, or the one you will create as an instance of

```scala
import com.kinja.amqp.{ AmqpClientRegistry, AmqpClientProvider }

trait ProductionAmqpClientProvider extends AmqpClientProvider {
	override lazy val amqpClientRegistry: AmqpClientRegistry = ProductionAmqpClientRegistry
}
```

### Example usage

Here's an example where you can send a message to RabbitMQ calling a controller action, and a consumer you can initiate and then we'll log the consumed messages to the console.

```scala
package com.kinja.presentation.controller

import com.kinja.common.logging.Logging
import com.kinja.presentation.dependencies._

import play.api.libs.json.Json
import play.api.mvc._

import scala.concurrent.Future

object RabbitPrototypeController
	extends Controller
	with AsyncActions
	with Logging
	with ProductionAmqpClientProvider {

	RabbitPrototypeConsumer.init()

	private val messageProducer = amqpClientRegistry.getMessageProducer("amq.topic")

	def publishToMQ(routingKey: String, message: String) = apiResponse(parse.tolerantText) { request =>
		val event = Map("message" -> message)
		messageProducer.publish(routingKey, event)
		logger.warn("[RabbitMQ] Published to queue with routing key: " + routingKey)
		Future.successful(Json.toJson("Ok"))
	}
}

object RabbitPrototypeConsumer extends Logging with ProductionAmqpClientProvider {

	def init(): Unit = {
		amqpClientRegistry.getMessageConsumer("test-messages").subscribe(consume)
	}

	def consume(message: Map[String, String]): Unit = {
		logger.warn("[RabbitMQ] Consumed message: " + message)
	}
}
```
