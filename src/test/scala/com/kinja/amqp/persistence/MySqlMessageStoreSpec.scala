package com.kinja.amqp.persistence

import com.kinja.amqp.model.{ Message, MessageConfirmation }

import org.specs2.execute.{ AsResult, Result }
import org.specs2._, specification.Scope
import org.specs2.concurrent.ExecutionEnv

import java.sql.Timestamp

class SessionRepositorySpec(implicit ee: ExecutionEnv) extends mutable.Specification with ParseInitSql {

	"saveMessage" should {
		"save a message" in new S {
			store.saveMessage(message)
			store.loadLockedMessages(100) === List(message)
		}
		"save a message several times with autoinc id" in new S {
			store.saveMessage(message)
			store.saveMessage(message)
			store.saveMessage(message)
			store.loadLockedMessages(100) === List(
				message.copy(id = Some(1)),
				message.copy(id = Some(2)),
				message.copy(id = Some(3)))
		}
	}

	trait S extends Scope with mutable.Around with H2Database {

		val processId = "test-store"

		val store = new MySqlMessageStore(processId, h2ds, h2ds)

		val ts1 = new Timestamp(System.currentTimeMillis - 5000)

		val ts2 = new Timestamp(System.currentTimeMillis - 2000)

		val message = Message(
			id = Some(1),
			routingKey = "routing-key",
			exchangeName = "exchange-name",
			message = "test-message",
			channelId = Some("channel-id"),
			deliveryTag = Some(1234L),
			createdTime = ts1,
			processedBy = Some(processId),
			lockedAt = Some(ts2))

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
