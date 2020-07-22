package com.kinja.amqp.impl.akkastream

import org.specs2.concurrent.ExecutionEnv
import org.specs2.matcher.FutureMatchers
import org.specs2.mutable

import scala.concurrent.duration._

class AtMostOnceProducerSpec(implicit ee: ExecutionEnv) extends mutable.Specification with FutureMatchers with ProducerContext {

	//The tests are designed to run in sequence!
	sequential

	"Producer" should {
		"send messages" in { factory: ProducerTestFactory =>
			val consumer = factory.createTestConsumer[String]
			val result = consumer.take[String](2)
			val producer = factory.createAtMostOnceProducer
			val routingKey = factory.defaultQueueWithRelatedParameters.bindingKey
			producer.publish(routingKey, "Msg1") must be_==().await
			producer.publish(routingKey, "Msg2") must be_==().await

			val expected = Seq[Either[Throwable, String]](Right("Msg1"), Right("Msg2"))

			result must be_==(expected).await(3, 3.seconds)
		}

		"shut down properly" in { _: ProducerTestFactory =>
			//As the producer context shut down the producer after every test, it is tested.
			success
		}
	}

}
