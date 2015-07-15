package com.kinja.amqp.persistence

import com.kinja.amqp.model.{ Message, MessageConfirmation }

import org.specs2.execute.{ AsResult, Result }
import org.specs2._, specification.Scope
import org.specs2.concurrent.ExecutionEnv

import org.scalacheck.{ Prop, Gen, Arbitrary }, Arbitrary.arbitrary

import java.sql.Timestamp

class SessionRepositorySpec(implicit ee: ExecutionEnv) extends mutable.Specification with ScalaCheck with ParseInitSql {

	private val processId = "test-store"

	private val ts1 = new Timestamp(System.currentTimeMillis - 5000)

	private val ts2 = new Timestamp(System.currentTimeMillis - 2000)

	private val message = Message(
		id = Some(1),
		routingKey = "routing-key",
		exchangeName = "exchange-name",
		message = "test-message",
		channelId = Some("channel-id-1"),
		deliveryTag = Some(1234L),
		createdTime = ts1,
		processedBy = Some(processId),
		lockedAt = Some(ts2))

	private val confirmation = MessageConfirmation(
		id = Some(1),
		channelId = "channel-id-2",
		deliveryTag = 4567L,
		multiple = true,
		createdTime = ts1)

	private val genMessage: Gen[Message] =
		for {
			id <- arbitrary[Long] map Math.abs
			routingKey <- Gen.alphaStr map (_.take(256))
			exchangeName <- Gen.alphaStr map (_.take(128))
			message <- Gen.alphaStr
			channelId <- Gen.alphaStr map (_.take(36))
			deliveryTag <- arbitrary[Int] map Math.abs
			ts1 = new Timestamp(System.currentTimeMillis - 5000)
			ts2 = new Timestamp(System.currentTimeMillis - 2000)
		} yield Message(
			id = Some(id),
			routingKey = routingKey,
			exchangeName = exchangeName,
			message = message,
			channelId = Some(channelId),
			deliveryTag = Some(deliveryTag),
			createdTime = ts1,
			processedBy = Some(processId),
			lockedAt = Some(ts2))

	private val genConfirmation: Gen[MessageConfirmation] =
		for {
			id <- arbitrary[Long] map (Math.abs)
			channelId <- Gen.alphaStr map (_.take(36))
			deliveryTag <- arbitrary[Int] map Math.abs
			multiple <- arbitrary[Boolean]
			ts = new Timestamp(System.currentTimeMillis - 5000)
		} yield MessageConfirmation(
			id = Some(id),
			channelId = channelId,
			deliveryTag = deliveryTag,
			multiple = multiple,
			createdTime = ts)

	private val genMessages: Gen[List[Message]] =
		for {
			n <- Gen.choose(1, 5)
			items <- Gen.listOfN(n, genMessage)
		} yield items

	private val genConfirmations: Gen[List[MessageConfirmation]] =
		for {
			n <- Gen.choose(1, 5)
			items <- Gen.listOfN(n, genConfirmation)
		} yield items

	private implicit val arbitraryMessage: Arbitrary[Message] = Arbitrary(genMessage)

	private implicit val arbitraryMessages: Arbitrary[List[Message]] = Arbitrary(genMessages)

	private implicit val arbitraryConfirmation: Arbitrary[MessageConfirmation] = Arbitrary(genConfirmation)

	private implicit val arbitraryConfirmations: Arbitrary[List[MessageConfirmation]] = Arbitrary(genConfirmations)

	"saveMessage" should {
		"save messages" in new S {
			val p = prop { (msg: Message) =>
				store.saveMessage(msg)
				val loaded = loadAllMessages
				// clean up
				deleteAllMessages
				// ignore autoinc id
				loaded.map(_.copy(id = None)) === List(msg.copy(id = None))
			}
			AsResult(p)
		}
		"save a message with undefined channelId" in new S {
			val msg = message.copy(channelId = None)
			store.saveMessage(msg)
			val loaded = loadAllMessages
			loaded === List(msg)
			loaded.head.channelId must beNone
		}
		"save a message with undefined deliveryTag" in new S {
			val msg = message.copy(deliveryTag = None)
			store.saveMessage(msg)
			val loaded = loadAllMessages
			loaded === List(msg)
			loaded.head.deliveryTag must beNone
		}
		"save a message with undefined processedBy" in new S {
			val msg = message.copy(processedBy = None)
			store.saveMessage(msg)
			val loaded = loadAllMessages
			loaded === List(msg)
			loaded.head.processedBy must beNone
		}
		"save a message with undefined lockedAt" in new S {
			val msg = message.copy(lockedAt = None)
			store.saveMessage(msg)
			val loaded = loadAllMessages
			loaded === List(msg)
			loaded.head.lockedAt must beNone
		}
		"save a message several times with autoinc id" in new S {
			store.saveMessage(message)
			store.saveMessage(message)
			store.saveMessage(message)
			loadAllMessages === List(
				message.copy(id = Some(1)),
				message.copy(id = Some(2)),
				message.copy(id = Some(3)))
		}
	}

	"saveMultipleMessages" should {
		"save messages" in new S {
			val p = prop { (msgs: List[Message]) =>
				store.saveMultipleMessages(msgs)
				val loaded = loadAllMessages
				// clean up
				deleteAllMessages
				// ignore autoinc id
				loaded.map(_.copy(id = None)) === msgs.map(_.copy(id = None))
			}
			AsResult(p)
		}
		"save multiple messages with autoinc id" in new S {
			store.saveMultipleMessages(List(message, message, message))
			loadAllMessages === List(
				message.copy(id = Some(1)),
				message.copy(id = Some(2)),
				message.copy(id = Some(3)))
		}
	}

	"saveConfirmation" should {
		"save confirmations" in new S {
			val p = prop { (conf: MessageConfirmation) =>
				store.saveConfirmation(conf)
				val loaded = loadAllConfirmations
				// clean up
				deleteAllConfirmations
				// ignore autoinc id
				loaded.map(_.copy(id = None)) === List(conf.copy(id = None))
			}
			AsResult(p)
		}
		"save a confirmation" in new S {
			store.saveConfirmation(confirmation)
			loadAllConfirmations === List(confirmation)
		}
		"save a confirmation several times with autoinc id" in new S {
			store.saveConfirmation(confirmation)
			store.saveConfirmation(confirmation)
			store.saveConfirmation(confirmation)
			loadAllConfirmations === List(
				confirmation.copy(id = Some(1)),
				confirmation.copy(id = Some(2)),
				confirmation.copy(id = Some(3)))
		}
	}

	"saveMultipleConfirmations" should {
		"save confirmations" in new S {
			val p = prop { (confs: List[MessageConfirmation]) =>
				store.saveMultipleConfirmations(confs)
				val loaded = loadAllConfirmations
				// clean up
				deleteAllConfirmations
				// ignore autoinc id
				loaded.map(_.copy(id = None)) === confs.map(_.copy(id = None))
			}
			AsResult(p)
		}
		"save a multiple confirmations with autoinc id" in new S {
			store.saveMultipleConfirmations(List(confirmation, confirmation, confirmation))
			loadAllConfirmations === List(
				confirmation.copy(id = Some(1)),
				confirmation.copy(id = Some(2)),
				confirmation.copy(id = Some(3)))
		}
	}

	trait S extends Scope with mutable.Around with H2Database {

		val store = new MySqlMessageStore(processId, h2ds, h2ds)

		import store._

		def loadAllMessages: List[Message] = onRead { implicit conn =>
			prepare("SELECT * FROM rabbit_messages") { stmt =>
				stmt.list[Message]
			}
		}

		def loadAllConfirmations: List[MessageConfirmation] = onRead { implicit conn =>
			prepare("SELECT * FROM rabbit_confirmations") { stmt =>
				stmt.list[MessageConfirmation]
			}
		}

		def deleteAllMessages: Int = onRead { implicit conn =>
			prepare("DELETE FROM rabbit_messages") { stmt =>
				stmt.executeUpdate
			}
		}

		def deleteAllConfirmations: Int = onRead { implicit conn =>
			prepare("DELETE FROM rabbit_confirmations") { stmt =>
				stmt.executeUpdate
			}
		}

		def around[T: AsResult](t: => T): Result = {
			initSql foreach { query =>
				val conn = h2ds.getConnection
				try {
					val stmt = conn.prepareStatement(query)
					try {
						stmt.executeUpdate
					} finally {
						if (stmt != null) stmt.close
					}
				} finally {
					if (conn != null) conn.close
				}
			}

			AsResult(t)
		}
	}
}
