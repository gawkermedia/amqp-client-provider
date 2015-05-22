package com.kinja.amqp.persistence

import java.sql.{ Date, Timestamp }
import java.text.SimpleDateFormat

import com.kinja.amqp.model.{ Message, MessageConfirmation }

import scala.concurrent.Future
import scala.slick.driver.ExtendedProfile
import scala.slick.jdbc.GetResult.GetLong
import scala.slick.jdbc.{ GetResult, StaticQuery }
import scala.slick.lifted.ColumnBase

abstract class MySqlMessageStore(
	processId: String,
	writeDs: javax.sql.DataSource,
	readDs: javax.sql.DataSource
) extends MessageStore {

	this: ExtendedProfile =>
	import simple._
	import Database.threadLocalSession

	import StaticQuery.interpolation

	val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

	implicit val getMessageConf = GetResult(r => MessageConfirmation(r.<<, r.<<, r.<<, r.<<, r.<<))
	implicit val getMessage = GetResult(r => Message(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))

	object MessageTable extends Table[Message]("rabbit_messages") {
		def id: Column[Long] = column[Long]("id", O.PrimaryKey, O.AutoInc)
		def exchangeName: Column[String] = column[String]("exchangeName")
		def routingKey: Column[String] = column[String]("routingKey")
		def message: Column[String] = column[String]("message")
		def channelId: Column[Option[String]] = column[Option[String]]("channelId")
		def deliveryTag: Column[Option[Long]] = column[Option[Long]]("deliveryTag")
		def createdTime: Column[Timestamp] = column[Timestamp]("createdTime")
		def processedBy: Column[Option[String]] = column[Option[String]]("processedBy")
		def lockedAt: Column[Option[Timestamp]] = column[Option[Timestamp]]("lockedAt")

		def * : ColumnBase[Message] =
			id.? ~ routingKey ~ exchangeName ~ message ~ channelId ~ deliveryTag ~ createdTime ~ processedBy ~ lockedAt <> (Message.apply _, Message.unapply _)

		def autoInc =
			id.? ~ routingKey ~ exchangeName ~ message ~ channelId ~ deliveryTag ~ createdTime ~ processedBy ~ lockedAt <> (Message.apply _, Message.unapply _) returning id
	}

	object MessageConfirmationTable extends Table[MessageConfirmation]("rabbit_confirmations") {
		def id: Column[Long] = column[Long]("id", O.PrimaryKey, O.AutoInc)
		def channelId: Column[String] = column[String]("channelId")
		def deliveryTag: Column[Long] = column[Long]("deliveryTag")
		def multiple: Column[Boolean] = column[Boolean]("multiple")
		def createdTime: Column[Timestamp] = column[Timestamp]("createdTime")

		def * : ColumnBase[MessageConfirmation] =
			id.? ~ channelId ~ deliveryTag ~ multiple ~ createdTime <> (MessageConfirmation.apply _, MessageConfirmation.unapply _)

		def autoInc =
			* returning id
	}

	private object Queries {
		def deleteMessagesWithMatchingMultiConfirms() =
			sqlu"""
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

		def deleteOldSingleConfirms(olderThan: String) =
			sqlu"""
				DELETE FROM
					rabbit_confirmations
				WHERE
					createdTime < $olderThan
					AND
	 				multiple = 0
			"""

		def lockRowsOlderThan(olderThan: String, currentDate: String, lockTimeOutAfter: String, limit: Int) =
			sqlu"""
			 	UPDATE
					rabbit_messages
				SET
					processedBy = $processId,
					lockedAt = $currentDate
	 			WHERE
	 				createdTime < $olderThan
					AND (processedBy IS NULL OR lockedAt < $lockTimeOutAfter)
				LIMIT $limit
			"""

		def deleteMatchingMessagesAndSingleConfirms() =
			sqlu"""
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

		def deleteMessageById(id: Long) =
			sqlu"""
				DELETE
			 		FROM rabbit_messages
					WHERE id=$id
			"""

		def deleteMultiConfIfNoMsg(olderThan: String) =
			sqlu"""
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
	  				c.createdTime < $olderThan
					AND
					m.id IS NULL
			"""

		def selectLockedMessages(limit: Int) =
			sql"""
				SELECT *
			 		FROM rabbit_messages
					WHERE `processedBy` = $processId
		 		LIMIT $limit
			""".as[Message]

		def selectMessageByChannelAndDelivery(channelId: String, deliveryTag: Long) = for {
			c <- MessageTable
			if c.channelId === channelId && c.deliveryTag === deliveryTag
		} yield c
	}

	override def saveMessage(msg: Message): Unit = {
		Database.forDataSource(writeDs).withSession {
			MessageTable.autoInc.insert(msg)
		}
	}

	override def saveConfirmation(confirm: MessageConfirmation): Unit = {
		Database.forDataSource(writeDs).withSession {
			MessageConfirmationTable.autoInc.insert(confirm)
		}
	}

	override def deleteMessageUponConfirm(channelId: String, deliveryTag: Long): Future[Boolean] =
		Database.forDataSource(writeDs).withSession {
			Future.successful(Queries.selectMessageByChannelAndDelivery(channelId, deliveryTag).delete > 0)
		}

	override def deleteMultiConfIfNoMatchingMsg(olderThanSeconds: Long): Int =
		Database.forDataSource(writeDs).withSession {
			val formatted = getFormattedDateForSecondsAgo(olderThanSeconds)
			Queries.deleteMultiConfIfNoMsg(formatted).first()
		}

	override def loadLockedMessages(limit: Int): List[Message] = {
		Database.forDataSource(readDs).withSession {
			Queries.selectLockedMessages(limit).list()
		}
	}

	override def deleteMessage(id: Long): Unit = {
		Database.forDataSource(writeDs).withSession {
			Queries.deleteMessageById(id).execute()
		}
	}

	override def deleteMatchingMessagesAndSingleConfirms(): Int = {
		Database.forDataSource(writeDs).withSession {
			Queries.deleteMatchingMessagesAndSingleConfirms().first()
		}
	}

	override def lockRowsOlderThan(olderThanSeconds: Long, lockTimeOutAfterSeconds: Long, limit: Int): Int = {
		Database.forDataSource(writeDs).withSession {
			Queries.lockRowsOlderThan(
				getFormattedDateForSecondsAgo(olderThanSeconds),
				getFormattedDateForSecondsAgo(0),
				getFormattedDateForSecondsAgo(lockTimeOutAfterSeconds),
				limit
			).first()
		}
	}

	override def deleteOldSingleConfirms(olderThanSeconds: Long): Int = {
		Database.forDataSource(writeDs).withSession {
			Queries.deleteOldSingleConfirms(getFormattedDateForSecondsAgo(olderThanSeconds)).first()
		}
	}

	override def deleteMessagesWithMatchingMultiConfirms(): Int = {
		Database.forDataSource(writeDs).withSession {
			Queries.deleteMessagesWithMatchingMultiConfirms().first()
		}
	}

	def saveMultipleMessages(messages: List[Message]): Unit = {
		Database.forDataSource(writeDs).withSession {
			MessageTable.autoInc.insertAll(messages: _*)
		}
	}

	def saveMultipleConfirmations(confirmations: List[MessageConfirmation]): Unit = {
		Database.forDataSource(writeDs).withSession {
			MessageConfirmationTable.autoInc.insertAll(confirmations: _*)
		}
	}

	private def getFormattedDateForSecondsAgo(seconds: Long): String = {
		val date = new Date(System.currentTimeMillis() - seconds * 1000)
		df.format(date)
	}
}
