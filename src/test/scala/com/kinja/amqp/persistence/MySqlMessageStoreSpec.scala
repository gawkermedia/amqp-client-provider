package com.kinja.amqp.persistence

import com.kinja.amqp.model.{ Message, MessageConfirmation }

import org.specs2.execute.{ AsResult, Result }
import org.specs2._, specification.Scope
import org.specs2.concurrent.ExecutionEnv

import org.scalacheck.{ Prop, Gen, Arbitrary }, Arbitrary.arbitrary

import java.sql.Timestamp

@SuppressWarnings(Array("org.brianmckenna.wartremover.warts.ExplicitImplicitTypes"))
class SessionRepositorySpec(implicit ee: ExecutionEnv) extends mutable.Specification with ScalaCheck with ParseInitSql {

	private val processId = "test-store"

	private def eraseId(m: Message): Message = m.copy(id = None)

	private def eraseId(c: MessageConfirmation): MessageConfirmation = c.copy(id = None)

	private def dt(delta: Int): Timestamp = new Timestamp(System.currentTimeMillis + delta * 1000)

	private val ts1: Timestamp = dt(-5)

	private val ts2: Timestamp = dt(-2)

	private val message = Message(
		id = Some(1),
		routingKey = "routing-key",
		exchangeName = "exchange-name",
		message = "test-message",
		channelId = Some("channel-id-1"),
		deliveryTag = Some(1234L),
		createdTime = ts1,
		processedBy = Some(processId),
		lockedAt = Some(ts2)
	)

	private val confirmation = MessageConfirmation(
		id = Some(1),
		channelId = "channel-id-2",
		deliveryTag = 4567L,
		multiple = true,
		createdTime = ts1
	)

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
			lockedAt = Some(ts2)
		)

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
			createdTime = ts
		)

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
				loaded.map(eraseId) === List(msg).map(eraseId)
			}
			AsResult(p)
		}
		"save a message with undefined channelId" in new S {
			val msg = message.copy(channelId = None)
			store.saveMessage(msg)
			val loaded = loadAllMessages
			loaded === List(msg)
			loaded.headOption.flatMap(_.channelId) must beNone
		}
		"save a message with undefined deliveryTag" in new S {
			val msg = message.copy(deliveryTag = None)
			store.saveMessage(msg)
			val loaded = loadAllMessages
			loaded === List(msg)
			loaded.headOption.flatMap(_.deliveryTag) must beNone
		}
		"save a message with undefined processedBy" in new S {
			val msg = message.copy(processedBy = None)
			store.saveMessage(msg)
			val loaded = loadAllMessages
			loaded === List(msg)
			loaded.headOption.flatMap(_.processedBy) must beNone
		}
		"save a message with undefined lockedAt" in new S {
			val msg = message.copy(lockedAt = None)
			store.saveMessage(msg)
			val loaded = loadAllMessages
			loaded === List(msg)
			loaded.headOption.flatMap(_.lockedAt) must beNone
		}
		"save a message several times with autoinc id" in new S {
			store.saveMessage(message)
			store.saveMessage(message)
			store.saveMessage(message)
			loadAllMessages === List(
				message.copy(id = Some(1)),
				message.copy(id = Some(2)),
				message.copy(id = Some(3))
			)
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
				loaded.map(eraseId) === msgs.map(eraseId)
			}
			AsResult(p)
		}
		"save multiple messages with autoinc id" in new S {
			store.saveMultipleMessages(List(message, message, message))
			loadAllMessages === List(
				message.copy(id = Some(1)),
				message.copy(id = Some(2)),
				message.copy(id = Some(3))
			)
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
				loaded.map(eraseId) === List(conf).map(eraseId)
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
				confirmation.copy(id = Some(3))
			)
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
				loaded.map(eraseId) === confs.map(eraseId)
			}
			AsResult(p)
		}
		"save a multiple confirmations with autoinc id" in new S {
			store.saveMultipleConfirmations(List(confirmation, confirmation, confirmation))
			loadAllConfirmations === List(
				confirmation.copy(id = Some(1)),
				confirmation.copy(id = Some(2)),
				confirmation.copy(id = Some(3))
			)
		}
	}

	"deleteMessageUponConfirm" should {
		"delete all messages by channelId and deliveryTag" in new S {
			val m1 = message.copy(id = Some(1), channelId = Some("channel-id"), deliveryTag = Some(1234L))
			val m2 = message.copy(id = Some(2), channelId = Some("channel-id"), deliveryTag = Some(1234L))
			val m3 = message.copy(id = Some(3), channelId = Some("something-else"), deliveryTag = Some(1234L))
			val m4 = message.copy(id = Some(4), channelId = Some("channel-id"), deliveryTag = Some(1234L))
			val m5 = message.copy(id = Some(5), channelId = Some("other-stuff"), deliveryTag = Some(1234L))
			val m6 = message.copy(id = Some(6), channelId = None, deliveryTag = Some(1234L))
			val m7 = message.copy(id = Some(7), channelId = Some("other-stuff"), deliveryTag = Some(5678L))
			val m8 = message.copy(id = Some(8), channelId = Some("other-stuff"), deliveryTag = Some(98233L))
			val m9 = message.copy(id = Some(9), channelId = Some("other-stuff"), deliveryTag = None)
			val m10 = message.copy(id = Some(10), channelId = None, deliveryTag = None)

			val toInsert = List(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10)
			val toKeep = List(m3, m5, m6, m7, m8, m9, m10)

			store.saveMultipleMessages(toInsert)

			// make sure all of them are loaded
			loadAllMessages === toInsert

			// now delete a few of them
			store.deleteMessageUponConfirm("channel-id", 1234L)

			// make sure everything else is kept
			loadAllMessages === toKeep
		}
	}

	"deleteMultiConfIfNoMatchingMsg" should {
		"work" in {
			// this can't be tested with H2 because it doesn't support joins in delete statements
			skipped
		}
	}

	"loadLockedMessages" should {
		"load all messages for the current process" in new S {
			val m1 = message.copy(id = Some(1), processedBy = Some(processId))
			val m2 = message.copy(id = Some(2), processedBy = Some("something-else"))
			val m3 = message.copy(id = Some(3), processedBy = Some(processId))
			val m4 = message.copy(id = Some(4), processedBy = Some(processId))
			val m5 = message.copy(id = Some(5), processedBy = None)
			val m6 = message.copy(id = Some(6), processedBy = Some(processId))
			val m7 = message.copy(id = Some(7), processedBy = Some(processId))

			val toInsert = List(m1, m2, m3, m4, m5, m6, m7)
			val toLoad = List(m1, m3, m4, m6)

			store.saveMultipleMessages(toInsert)

			// make sure all of them are loaded
			loadAllMessages === toInsert

			// skip the fifth
			store.loadLockedMessages(4) === toLoad
		}
	}

	"deleteMessage" should {
		"delete a message by id" in new S {
			val m1 = message.copy(id = Some(1))
			val m2 = message.copy(id = Some(2))
			val m3 = message.copy(id = Some(3))
			val m4 = message.copy(id = Some(4))
			val m5 = message.copy(id = Some(5))
			val m6 = message.copy(id = Some(6))
			val m7 = message.copy(id = Some(7))

			val toInsert = List(m1, m2, m3, m4, m5, m6, m7)

			store.saveMultipleMessages(toInsert)

			// make sure all of them are loaded
			loadAllMessages === toInsert

			store.deleteMessage(2)
			loadAllMessages === toInsert.filterNot(m => Set[Long](2).contains(m.id.getOrElse(0)))

			store.deleteMessage(6)
			loadAllMessages === toInsert.filterNot(m => Set[Long](2, 6).contains(m.id.getOrElse(0)))

			store.deleteMessage(3)
			loadAllMessages === toInsert.filterNot(m => Set[Long](2, 6, 3).contains(m.id.getOrElse(0)))

			store.deleteMessage(1)
			loadAllMessages === toInsert.filterNot(m => Set[Long](2, 6, 3, 1).contains(m.id.getOrElse(0)))

			store.deleteMessage(7)
			loadAllMessages === toInsert.filterNot(m => Set[Long](2, 6, 3, 1, 7).contains(m.id.getOrElse(0)))

			// delete again
			store.deleteMessage(6)
			loadAllMessages === toInsert.filterNot(m => Set[Long](2, 6, 3, 1, 7).contains(m.id.getOrElse(0)))
		}
		"not do anything with an empty table" in new S {
			loadAllMessages === List()
			store.deleteMessage(1)
			loadAllMessages === List()
		}
	}

	"deleteMatchingMessagesAndSingleConfirms" should {
		"work" in new S {
			// this can't be tested with H2 because it doesn't support joins in delete statements
			skipped
		}
	}

	"lockRowsOlderThan" should {

		"update a record if processedBy is null" in new S {
			val p = prop { (msg: Message) =>
				val start = System.currentTimeMillis / 1000 * 1000 // erase millis

				// make sure the record passes the createdTime check and processedBy is null
				store.saveMessage(msg.copy(createdTime = dt(-10), processedBy = None))

				store.lockRowsOlderThan(5, 5, 100)

				val loaded = loadAllMessages
				loaded.flatMap(_.processedBy) === List(processId)
				loaded.headOption.flatMap(_.lockedAt).map(_.getTime).getOrElse(0L) must be_>=(start)

				deleteAllMessages
				success
			}
			AsResult(p)
		}
		"update a record if lockedAt is older" in new S {
			val p = prop { (msg: Message) =>
				val start = System.currentTimeMillis / 1000 * 1000 // erase millis

				// make sure the record passes the createdTime check and processedBy is null
				store.saveMessage(msg.copy(createdTime = dt(-10), lockedAt = Some(dt(-8))))

				store.lockRowsOlderThan(5, 5, 100)

				val loaded = loadAllMessages
				loaded.flatMap(_.processedBy) === List(processId)
				loaded.headOption.flatMap(_.lockedAt).map(_.getTime).getOrElse(0L) must be_>=(start)

				deleteAllMessages
				success
			}
			AsResult(p)
		}
		"not update record if createdTime is newer" in new S {
			val p = prop { (msg: Message) =>
				val start = System.currentTimeMillis / 1000 * 1000 // erase millis

				// make sure both processedBy and lockedAt passes
				store.saveMessage(msg.copy(createdTime = dt(-2), processedBy = None, lockedAt = Some(dt(-8))))

				store.lockRowsOlderThan(5, 5, 100)

				val loaded = loadAllMessages
				loaded.flatMap(_.processedBy) === List()
				loaded.headOption.flatMap(_.lockedAt).map(_.getTime).getOrElse(0L) must be_<(start)

				deleteAllMessages
				success
			}
			AsResult(p)
		}
	}

	"deleteOldSingleConfirms" should {
		"delete all old confirmations that are not multiple" in new S {
			val c1 = confirmation.copy(id = Some(1), createdTime = dt(-20), multiple = false)
			val c2 = confirmation.copy(id = Some(2), createdTime = dt(-19), multiple = true)
			val c3 = confirmation.copy(id = Some(3), createdTime = dt(-18), multiple = false)
			val c4 = confirmation.copy(id = Some(4), createdTime = dt(-17), multiple = true)
			val c5 = confirmation.copy(id = Some(5), createdTime = dt(-10), multiple = false)
			val c6 = confirmation.copy(id = Some(6), createdTime = dt(-9), multiple = false)
			val c7 = confirmation.copy(id = Some(7), createdTime = dt(-8), multiple = true)

			val toInsert = List(c1, c2, c3, c4, c5, c6, c7)

			store.saveMultipleConfirmations(toInsert)

			loadAllConfirmations === toInsert

			store.deleteOldSingleConfirms(12)

			loadAllConfirmations === List(c2, c4, c5, c6, c7)
		}
	}

	"deleteMessagesWithMatchingMultiConfirms" should {
		"work" in new S {
			// this can't be tested with H2 because it doesn't support joins in delete statements
			skipped
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
						if (Option(stmt).isDefined) stmt.close
					}
				} finally {
					if (Option(conn).isDefined) conn.close
				}
			}

			AsResult(t)
		}
	}
}
