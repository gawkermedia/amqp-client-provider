package com.kinja.amqp.persistence

import java.sql.{ Date, Timestamp }
import java.text.SimpleDateFormat

import com.kinja.amqp.model.{ Message, MessageConfirmation }

import scala.concurrent.Future
import scala.collection.mutable.ListBuffer

import java.sql.{ Connection, PreparedStatement, ResultSet, Types }

class MySqlMessageStore(
	processId: String,
	override val writeDs: javax.sql.DataSource,
	override val readDs: javax.sql.DataSource) extends MessageStore with ORM {

	private val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

	private val messageFields = List(
		"id",
		"routingKey",
		"exchangeName",
		"message",
		"channelId",
		"deliveryTag",
		"createdTime",
		"processedBy",
		"lockedAt"
	)

	private val confirmationFields = List(
		"id",
		"channelId",
		"deliveryTag",
		"multiple",
		"createdTime"
	)

	implicit val getMessage = GetResult(r => Message(
		id = {
			val value = r.getLong("id")
			if (r.wasNull) None else Option(value)
		},
		routingKey = r.getString("routingKey"),
		exchangeName = r.getString("exchangeName"),
		message = r.getString("message"),
		channelId = {
			val value = r.getString("channelId")
			if (r.wasNull) None else Option(value)
		},
		deliveryTag = {
			val value = r.getLong("deliveryTag")
			if (r.wasNull) None else Option(value)
		},
		createdTime = r.getTimestamp("createdTime"),
		processedBy = {
			val value = r.getString("processedBy")
			if (r.wasNull) None else Option(value)
		},
		lockedAt = {
			val value = r.getTimestamp("lockedAt")
			if (r.wasNull) None else Option(value)
		}
	))

	implicit val getConfirmation = GetResult(r => MessageConfirmation(
		id = {
			val value = r.getLong("id")
			if (r.wasNull) None else Option(value)
		},
		channelId = r.getString("channelId"),
		deliveryTag = r.getLong("deliveryTag"),
		multiple = r.getBoolean("multiple"),
		createdTime = r.getTimestamp("createdTime")
	))

	implicit val setMessageAutoInc = SetResult[Message] { (stmt, message) =>
		stmt.setNull(1, Types.BIGINT) // NULL for autoinc
		stmt.setString(2, message.routingKey)
		stmt.setString(3, message.exchangeName)
		stmt.setString(4, message.message)
		message.channelId.map(v => stmt.setString(5, v)).getOrElse(stmt.setNull(5, Types.VARCHAR))
		message.deliveryTag.map(v => stmt.setLong(6, v)).getOrElse(stmt.setNull(6, Types.INTEGER))
		stmt.setTimestamp(7, message.createdTime)
		message.processedBy.map(v => stmt.setString(8, v)).getOrElse(stmt.setNull(8, Types.VARCHAR))
		message.lockedAt.map(v => stmt.setTimestamp(9, v)).getOrElse(stmt.setNull(9, Types.TIMESTAMP))
	}

	implicit val setConfirmationAutoInc = SetResult[MessageConfirmation] { (stmt, confirm) =>
		stmt.setNull(1, Types.BIGINT) // NULL for autoinc
		stmt.setString(2, confirm.channelId)
		stmt.setLong(3, confirm.deliveryTag)
		stmt.setBoolean(4, confirm.multiple)
		stmt.setTimestamp(5, confirm.createdTime)
	}

	private object Queries {

		val deleteMessagesWithMatchingMultiConfirms =
			"""
				DELETE
					m.*
				FROM
					rabbit_messages m
				JOIN
					rabbit_confirmations c
						ON m.channelId = c.channelId
						AND m.deliveryTag <= c.deliveryTag
				WHERE
					c.multiple = 1
			"""

		val deleteOldSingleConfirms =
			"""
				DELETE FROM
					rabbit_confirmations
				WHERE
					createdTime < ?
					AND
	 				multiple = 0
			"""

		val lockRowsOlderThan =
			"""
			 	UPDATE
					rabbit_messages
				SET
					processedBy = ?,
					lockedAt = ?
				WHERE
					createdTime < ?
					AND (processedBy IS NULL OR lockedAt < ?)
				LIMIT ?
			"""

		val deleteMatchingMessagesAndSingleConfirms =
			"""
				DELETE
					m.*, c.*
				FROM
					rabbit_messages m
				JOIN
					rabbit_confirmations c
						ON m.channelId = c.channelId
						AND m.deliveryTag = c.deliveryTag
				WHERE
					c.multiple = 0
			"""

		val deleteMessageById =
			"""
				DELETE
			 		FROM rabbit_messages
					WHERE id=?
			"""

		val deleteMultiConfIfNoMsg =
			"""
				DELETE
					c.*
				FROM
					rabbit_confirmations c
				LEFT JOIN
					rabbit_messages m
						ON m.channelId = c.channelId
						AND m.deliveryTag <= c.deliveryTag
				WHERE
					c.multiple = 1
	 				AND
					c.createdTime < ?
					AND
					m.id IS NULL
			"""

		val selectLockedMessages =
			"""
				SELECT *
			 		FROM rabbit_messages
					WHERE `processedBy` = ?
		 		LIMIT ?
			"""

		val deleteMessageByChannelAndDelivery =
			"""
				DELETE
					FROM rabbit_messages
					WHERE channelId = ?
						AND deliveryTag = ?
			"""

		val insertMessage =
			s"""
				INSERT INTO rabbit_messages (${messageFields.mkString(",")})
				VALUES (${questionmarks(messageFields)})
			"""

		val insertConfirmation =
			s"""
				INSERT INTO rabbit_confirmations (${confirmationFields.mkString(",")})
				VALUES (${questionmarks(confirmationFields)})
			"""
	}

	override def saveMessage(msg: Message): Unit = onWrite { implicit conn =>
		prepare(Queries.insertMessage) { stmt =>
			stmt.insert(msg)
		}
	}

	override def saveConfirmation(confirm: MessageConfirmation): Unit = onWrite { implicit conn =>
		prepare(Queries.insertConfirmation) { stmt =>
			stmt.insert(confirm)
		}
	}

	override def deleteMessageUponConfirm(channelId: String, deliveryTag: Long): Future[Boolean] = onWrite { implicit conn =>
		prepare(Queries.deleteMessageByChannelAndDelivery) { stmt =>
			stmt.setString(1, channelId)
			stmt.setLong(2, deliveryTag)
			Future.successful(stmt.executeUpdate > 0)
		}
	}

	override def deleteMultiConfIfNoMatchingMsg(olderThanSeconds: Long): Int = onWrite { implicit conn =>
		prepare(Queries.deleteMultiConfIfNoMsg) { stmt =>
			stmt.setString(1, getFormattedDateForSecondsAgo(olderThanSeconds))
			stmt.executeUpdate
		}
	}

	override def loadLockedMessages(limit: Int): List[Message] = onRead { implicit conn =>
		prepare(Queries.selectLockedMessages) { stmt =>
			stmt.setString(1, processId)
			stmt.setLong(2, limit)
			stmt.list[Message]
		}
	}

	override def deleteMessage(id: Long): Unit = onWrite { implicit conn =>
		prepare(Queries.deleteMessageById) { stmt =>
			stmt.setLong(1, id)
			stmt.executeUpdate
		}
	}

	override def deleteMatchingMessagesAndSingleConfirms(): Int = onWrite { implicit conn =>
		prepare(Queries.deleteMatchingMessagesAndSingleConfirms) { stmt =>
			stmt.executeUpdate
		}
	}

	override def lockRowsOlderThan(olderThanSeconds: Long, lockTimeOutAfterSeconds: Long, limit: Int): Int = onWrite { implicit conn =>
		prepare(Queries.lockRowsOlderThan) { stmt =>
			stmt.setString(1, processId)
			stmt.setString(2, getFormattedDateForSecondsAgo(0))
			stmt.setString(3, getFormattedDateForSecondsAgo(olderThanSeconds))
			stmt.setString(4, getFormattedDateForSecondsAgo(lockTimeOutAfterSeconds))
			stmt.setLong(5, limit)
			stmt.executeUpdate
		}
	}

	override def deleteOldSingleConfirms(olderThanSeconds: Long): Int = onWrite { implicit conn =>
		prepare(Queries.deleteOldSingleConfirms) { stmt =>
			stmt.setString(1, getFormattedDateForSecondsAgo(olderThanSeconds))
			stmt.executeUpdate
		}
	}

	override def deleteMessagesWithMatchingMultiConfirms(): Int = onWrite { implicit conn =>
		prepare(Queries.deleteMessagesWithMatchingMultiConfirms) { stmt =>
			stmt.executeUpdate
		}
	}

	def saveMultipleMessages(messages: List[Message]): Unit = onWrite { implicit conn =>
		prepare(Queries.insertMessage) { stmt =>
			stmt.insertAll(messages)
		}
	}

	def saveMultipleConfirmations(confirmations: List[MessageConfirmation]): Unit = onWrite { implicit conn =>
		prepare(Queries.insertConfirmation) { stmt =>
			stmt.insertAll(confirmations)
		}
	}

	private def getFormattedDateForSecondsAgo(seconds: Long): String = {
		val date = new Date(System.currentTimeMillis() - seconds * 1000)
		df.format(date)
	}

	override def shutdown(): Unit = {}
}
