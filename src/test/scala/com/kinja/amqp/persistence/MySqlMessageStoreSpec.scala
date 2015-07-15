package com.kinja.amqp.persistence

import com.kinja.amqp.model.{ Message, MessageConfirmation }

import org.scalatest._

import java.sql.Timestamp

abstract class UnitSpec extends FlatSpec with Matchers with OptionValues with Inside with Inspectors

class MySqlMessageStoreSpec extends UnitSpec with ParseInitSql {

	"saveMessage" should "save a message" in new S {
		store.saveMessage(message)
		store.loadLockedMessages(100) === List(message)
	}

	it should "save a message several times with autoinc id" in new S {
		store.saveMessage(message)
		store.saveMessage(message)
		store.saveMessage(message)
		store.loadLockedMessages(100) === List(
			message.copy(id = Some(1)),
			message.copy(id = Some(2)),
			message.copy(id = Some(3)))
	}

	"saveMultipleMessages" should "save a multiple messages with autoinc id" in new S {
		store.saveMultipleMessages(List(message, message, message))
		store.loadLockedMessages(100) === List(
			message.copy(id = Some(1)),
			message.copy(id = Some(2)),
			message.copy(id = Some(3)))
	}

	"saveConfirmation" should "save a confirmation" in new S {
		store.saveConfirmation(confirmation)
		store.loadConfirmations(100) === List(confirmation)
	}

	it should "save a confirmation several times with autoinc id" in new S {
		store.saveConfirmation(confirmation)
		store.saveConfirmation(confirmation)
		store.saveConfirmation(confirmation)
		store.loadConfirmations(100) === List(
			confirmation.copy(id = Some(1)),
			confirmation.copy(id = Some(2)),
			confirmation.copy(id = Some(3)))
	}

	"saveMultipleConfirmations" should "save a multiple confirmations with autoinc id" in new S {
		store.saveMultipleConfirmations(List(confirmation, confirmation, confirmation))
		store.loadConfirmations(100) === List(
			confirmation.copy(id = Some(1)),
			confirmation.copy(id = Some(2)),
			confirmation.copy(id = Some(3)))
	}

	trait S extends H2Database {

		val processId = "test-store"

		val store = new MySqlMessageStore(processId, h2ds, h2ds)

		val ts1 = new Timestamp(System.currentTimeMillis - 5000)

		val ts2 = new Timestamp(System.currentTimeMillis - 2000)

		val message = Message(
			id = Some(1),
			routingKey = "routing-key",
			exchangeName = "exchange-name",
			message = "test-message",
			channelId = Some("channel-id-1"),
			deliveryTag = Some(1234L),
			createdTime = ts1,
			processedBy = Some(processId),
			lockedAt = Some(ts2))

		val confirmation = MessageConfirmation(
			id = Some(1),
			channelId = "chann el-id-2",
			deliveryTag = 4567L,
			multiple = true,
			createdTime = ts1)

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
	}
}
