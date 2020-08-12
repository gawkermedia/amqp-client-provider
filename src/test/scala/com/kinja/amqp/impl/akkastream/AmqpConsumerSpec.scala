package com.kinja.amqp.impl.akkastream

import com.kinja.amqp.{ DeserializationException, Reads, ignore }
import akka.Done
import akka.actor.{ Actor, ActorRef, Props }
import akka.stream.alpakka.amqp.WriteResult
import akka.pattern.{ ask, pipe }
import akka.testkit.TestProbe
import akka.util.Timeout
import org.specs2._
import org.specs2.matcher._
import org.specs2.concurrent.ExecutionEnv

import scala.concurrent.Future
import scala.concurrent.duration._

class AmqpConsumerSpec(implicit ee: ExecutionEnv) extends mutable.Specification with FutureMatchers with ConsumerContext {

	private def msgExtractor(testProbe: TestProbe): String => Future[Unit] = s => Future.successful(testProbe.ref ! s)

	def stateProps(testProbe: TestProbe): Props = Props(new State(testProbe))

	class State(testProbe: TestProbe) extends Actor {

		@SuppressWarnings(Array("org.wartremover.warts.Var"))
		var msgs: Map[String, Option[ActorRef]] = Map.empty[String, Option[ActorRef]].withDefaultValue(None)

		private implicit val ex = context.dispatcher

		override def receive: Receive = {
			case s: String =>
				msgs += s -> Option(sender())
				testProbe.ref ! s
			case (Done, msg: String) =>
				msgs(msg).map(_ ! Done)
				msgs -= msg
			case (t: Throwable, msg: String) =>
				ignore(msgs(msg).map(r => Future.failed[Done](t) pipeTo r))
		}
	}

	private def waiter(state: ActorRef): String => Future[Unit] = s => {
		(state ? s)(Timeout(1.minute)).map(_ => ())(system.dispatcher)
	}

	//The tests are designed to run in sequence!
	sequential

	"Consumer" should {
		"receive messages sent by producers" in { factory: ConsumerTestFactory =>
			val consumer = factory.createConsumer
			val testProbe = TestProbe()
			consumer.subscribe[String](1.seconds)(msgExtractor(testProbe))

			val producer = factory.createTestProducer[String]

			producer.send("Msg1") must be_==(WriteResult.confirmed).await
			producer.send("Msg2") must be_==(WriteResult.confirmed).await
			Seq("Msg1", "Msg2") === testProbe.expectMsgAllOf(1.seconds, "Msg1", "Msg2")
		}

		"be able to set their channel's prefetch size" in { factory: ConsumerTestFactory =>
			val config = factory.defaultConsumerConfig.copy(defaultPrefetchCount = 3)
			val consumer = factory.createConsumer(config, factory.defaultQueueWithRelatedParameters)
			val probe = TestProbe()
			val processingState = system.actorOf(stateProps(probe))
			consumer.subscribe[String](5.seconds)(waiter(processingState))

			val producer = factory.createTestProducer[String]

			val msg1 = "Msg1"
			producer.send(msg1) must be_==(WriteResult.confirmed).await
			msg1 === probe.expectMsg(200.millis, msg1)
			val msg2 = "Msg2"
			producer.send(msg2) must be_==(WriteResult.confirmed).await
			msg2 === probe.expectMsg(200.millis, msg2)
			val msg3 = "Msg3"
			producer.send(msg3) must be_==(WriteResult.confirmed).await
			msg3 === probe.expectMsg(200.millis, msg3)

			// we have 3 pending messages, this one should not be received
			val msg4 = "Msg4"
			producer.send(msg4) must be_==(WriteResult.confirmed).await
			probe.expectNoMessage(50.millis)

			// but if we ack one our our messages we should get the 4th delivery
			processingState ! ((Done, msg1))
			probe.expectMsg(200.millis, msg4)
			processingState ! ((Done, msg2))
			processingState ! ((Done, msg3))
			processingState ! ((Done, msg4))
			success
		}

		"receive message again if the processing failed" in { factory: ConsumerTestFactory =>
			val config = factory.defaultConsumerConfig.copy(reconnectionTime = 1.seconds)
			val consumer = factory.createConsumer(config, factory.defaultQueueWithRelatedParameters)
			val probe = TestProbe()
			val processingState = system.actorOf(stateProps(probe))
			consumer.subscribe[String](5.seconds)(waiter(processingState))

			val producer = factory.createTestProducer[String]

			val msg1 = "Msg1"

			producer.send(msg1) must be_==(WriteResult.confirmed).await
			msg1 === probe.expectMsg(200.millis, msg1)
			processingState ! (((new Exception("processingFailed"), msg1)))
			msg1 === probe.expectMsg(200.millis, msg1)
			processingState ! ((Done, msg1))
			probe.expectNoMessage(200.millis)
			success
		}

		"skip processing message if it is unreadable" in { factory: ConsumerTestFactory =>
			val config = factory.defaultConsumerConfig.copy(reconnectionTime = 1.seconds)
			val consumer = factory.createConsumer(config, factory.defaultQueueWithRelatedParameters)
			val probe = TestProbe()
			implicit val reads: Reads[String] = new Reads[String] {
				override def reads(s: String): Either[DeserializationException, String] = s match {
					case "Failure" => Left[DeserializationException, String](new DeserializationException("Failure message is not serializable"))
					case s => Right[DeserializationException, String](s)
				}
			}
			consumer.subscribe[String](5.seconds)(msgExtractor(probe))

			val producer = factory.createTestProducer[String]

			val msg1 = "msg1"
			val failureMsg = "Failure"
			val msg2 = "msg2"
			producer.send(msg1) must be_==(WriteResult.confirmed).await
			producer.send(failureMsg) must be_==(WriteResult.confirmed).await
			producer.send(msg2) must be_==(WriteResult.confirmed).await

			Seq(msg1, msg2) === probe.expectMsgAllOf[String](200.millis, msg1, msg2)
			probe.expectNoMessage(200.millis)
			success
		}

		"declare queues and bindings" in { factory: ConsumerTestFactory =>
			val config = factory.defaultConsumerConfig.copy(reconnectionTime = 100.millis)
			val queueParameters = factory.defaultQueueWithRelatedParameters

			//Delete binding, queue and exchange
			factory.deleteBindingQueueAndExchange(queueParameters)
			val consumer = factory.createConsumer(config, queueParameters)
			val probe = TestProbe()
			consumer.subscribe[String](5.seconds)(msgExtractor(probe))
			Thread.sleep(100) // IMPORTANT!! Wait for topology recovery as test producer not handling failure well!
			val producer = factory.createTestProducer[String](factory.connectionProvider, queueParameters)
			val testMsg = "testMsg"
			producer.send(testMsg) must be_==(WriteResult.confirmed).await
			testMsg === probe.expectMsg(100.millis, testMsg)
			producer.shutdown must be_==(Done).await
			//Delete binding, queue and exchange again!
			factory.deleteBindingQueueAndExchange(queueParameters)
			//Waiting for recreation of binding
			Thread.sleep(200) // IMPORTANT!! Wait for topology recovery as test producer not handling failure well!
			val producer2 = factory.createTestProducer[String](factory.connectionProvider, queueParameters)
			val testMsg2 = "testMsg2"
			producer2.send(testMsg2) must be_==(WriteResult.confirmed).await
			testMsg2 === probe.expectMsg(100.millis, testMsg2)

		}

		"shut down properly" in { _: ConsumerTestFactory =>
			//As the consumer context shut down the consumer after every test, it is tested.
			success
		}
	}
}
