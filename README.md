# What is this?

This is an abstract layer between https://github.com/gawkermedia/amqp-client and your application.

# What does it do?

Enables various delivery guarantees, provided by message stores.

# How does it do it?

### Handles publisher confirmations and message republishing if messages weren't confirmed

Every message sent to RabbitMQ (we send messages with persistent flag by default) gets saved to the configurable `MessageStore`.
Then if the RabbitMQ sends back a confirmation that the message was persisted on the broker side, the message gets deleted. If no confirmation arrived within the configured timeframe, the `Repeater` resends with the same `Publisher`. The message will be picked up by the resend loop until it finally gets confirmed.

### Automatically sends consumer confirmations after the message was processed

The only thing you need to provide is a simple configuration and a function of `A => Future[Unit]`, and the library will handle the rest: create the queue and the exchange if they does not exist, create the binding, and start consuming messages. With every message, your function gets called. After processing the message, the library will send back the acknowledgement to RabbitMQ. If your code throws exception, the library sends back a negative acknowledgement and the message will be requeued. 

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
			at-least-once = ""
		}
	}
	builtinAtLeastOnceGroup = ""
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
		messageBatchSize = 30
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
  * `at-least-once`: Delivery guarantee used for this exchange. Default value is assumed if this is omitted. It's referred in code as `AtLeastOnceGroup.default`.
* `builtinAtLeastOnceGroup`: Delivery guarantee used for several predefined exchanges, like `amq.topic`. Default value is assumed if this is omitted. It's referred in code as `AtLeastOnceGroup.default`.
* `queues`: The list of queues you want to consume messages from. You can declare the queue's name (the index of the queue configuration), the exchange you want to bind the queue to, and the binding key for the binging. Your options of configuration are:
  * `exchange`: The exchange name to bind to queue to. It must exist in the `exchanges` above or be one of the built in exchange, list amq.topic
  * `routingKey`: The routing key for the binding.
* `resendLoop`: To ensure at-least-once-delivery and that the application is functional while RabbitMQ isn't functional, there's a message buffer where unconfirmed messages are stored until we get back confirmation from RabbitMQ. TODO more about this link to documentaion on that. TODO: set these values as default, so there's no need for all of these values in config as this works fine.
  * `republishTimeoutInSec`: Timeout in seconds for republishing an unconfirmed message. Set it higher then `askTimeOutInMilliSec`. TODO: think about getting rid of it and use a value based on `askTimeOutInMilliSec`
  * `initialDelayInSec`: The number of seconds to wait before starting processing the message buffer
  * `bufferProcessIntervalInSec`: The number of seconds between processing message buffer
  * `messageBatchSize`: The number of messages to resend in one batch.
  * `memoryFlushIntervalInMilliSec`: There's an in-memory buffer on top of the MySQL backed message buffer. This is the interval which after the messages got flushed to MySQL after they were published.
  * `memoryFlushChunkSize`: The number of messages got flushed to MySQL in one batch.
  * `memoryFlushTimeOutInSec`: The timeout of one batch of flush.
  
# How do I use it?

### Setup

First, at least one message store is needed. It would be responsible for actually implementing the delivery guarantee. One can be created using `rmq-storage-mysql` project, for example. We would assume that one indeed was created under the name `MyMessageStore`.

We need to create a new client.

```scala
class RabbitMQClientFactory {

	private implicit val ec: ExecutionContext = ExecutionContext.global

	private val logger = Logger("rabbitmq").logger

	/**
	 * The map of all message stores used. The keys in the map are delivery guarantees.
	 * Any exchange that has a delivery guarantee not listed in that map
	 * would not be using a message store at all,
	 * and therefore would use the at-most-once stratagy.
	 */
	private val stores = Map(AtLeastOnceGroup.default -> MyMessageStore)


	/**
	 * The RabbitMQ client instance.
	 */
	val client: AmqpClientInterface =
		new AmqpClientFactory().createClient(
			new AmqpConfiguration {
				protected override lazy val config: Config = configuration.underlying
			},
			play.libs.Akka.system,
			logger,
			ec,
			stores
		)

	// Start the message repeater that provides delivery guarantees.
	client.startMessageRepeater()
}
```


### Example usage

Here's an example where you can send a message to RabbitMQ calling a controller action, and a consumer you can initiate and then we'll log the consumed messages to the console.

```scala
object RabbitPrototypeController extends Controller with Logging {

	import RabbitMQSerialization._

	private val rabbitmqClient = new RabbitMQClientFactory.client

	private val messageProducer = rabbitmqClient.getMessageProducer("amq.topic")

	private val messageConsumer = rabbitmqClient.getMessageConsumer("test-messages")

	def publishToMQ(message: String) = Public { request =>
		messageProducer.publish("test.messages", message).toAccumulator.map {_ =>
			logger.warn("[RabbitMQ] Published message: " + message)
			Json.toJson("Ok")
		}
	}

	messageConsumer.subscribe[String, Unit](30.seconds) {
		logger.warn("[RabbitMQ] Consumed message: " + message)
	}
}
```
