package com.kinja.amqp.persistence

import java.sql.Date
import java.text.SimpleDateFormat

import com.kinja.amqp.model.{Message, MessageConfirmation}

import scala.slick.driver.ExtendedProfile
import scala.slick.jdbc.GetResult.GetLong
import scala.slick.jdbc.{GetResult, StaticQuery}
import scala.slick.lifted.{BaseTypeMapper, ColumnBase}

abstract class MySqlMessageStore(processId: String) extends MessageStore {

	this: ExtendedProfile =>
	import simple._
	import Database.threadLocalSession

	import StaticQuery.interpolation

	def writeDs: javax.sql.DataSource

	val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

	implicit val getDateResult = GetResult(r => new Date(GetLong(r)))
	implicit val getMessageConf = GetResult(r => MessageConfirmation(r.<<, r.<<, r.<<, r.<<, r.<<))
	implicit val getMessage = GetResult(r => Message(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))

	implicit object DateMapper extends MappedTypeMapper[Date, java.sql.Timestamp] with BaseTypeMapper[Date] {
		def map(lh: Date) = new java.sql.Timestamp(lh.getTime)
		def comap(rh: java.sql.Timestamp) = new Date(rh.getTime)
	}

	object MessageTable extends Table[Message]("rabbit_messages") {
		def id: Column[Long] = column[Long]("id", O.PrimaryKey, O.AutoInc)
		def exchangeName: Column[String] = column[String]("exchangeName")
		def routingKey: Column[String] = column[String]("routingKey")
		def message: Column[String] = column[String]("message")
		def channelId: Column[Option[String]] = column[Option[String]]("channelId")
		def deliveryTag: Column[Option[Long]] = column[Option[Long]]("deliveryTag")
		def createdTime: Column[Date] = column[Date]("createdTime")
		def processedBy: Column[Option[String]] = column[Option[String]]("processedBy")
		def lockedAt: Column[Option[Date]] = column[Option[Date]]("lockedAt")

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
		def createdTime: Column[Date] = column[Date]("createdTime")

		def * : ColumnBase[MessageConfirmation] =
			id.? ~ channelId ~ deliveryTag ~ multiple ~ createdTime <> (MessageConfirmation.apply _, MessageConfirmation.unapply _)

		def autoInc =
			id.? ~ channelId ~ deliveryTag ~ multiple ~ createdTime <> (MessageConfirmation.apply _, MessageConfirmation.unapply _) returning id
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

		def deleteSingleConfIfNoMatchingMsg(olderThan: String) =
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

		def deleteConfById(id: Long) =
			sqlu"""
				DELETE
			 		FROM rabbit_confirmations
					WHERE id=$id
			"""

		def deleteMessageById(id: Long) =
			sqlu"""
				DELETE
			 		FROM rabbit_messages
					WHERE id=$id
			"""

		def deleteMultiConfIfNoMsg(formattedTime: String) =
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
					m.id IS NULL
			"""

		def selectLockedMessages(limit: Int) =
			sql"""
				SELECT *
			 		FROM rabbit_messages
					WHERE `processedBy` = $processId
		 		LIMIT $limit
			""".as[Message]

		def selectConfByChannelIds(ids: List[String]) = for {
			c <- MessageConfirmationTable
			if c.channelId inSet ids
		} yield c

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

	override def saveConfirmation(conf: MessageConfirmation): Unit = {
		Database.forDataSource(writeDs).withSession {
			MessageConfirmationTable.autoInc.insert(conf)
		}
	}

	override def deleteMessageUponConfirm(channelId: String, deliveryTag: Long): Int =
		Database.forDataSource(writeDs).withSession {
			Queries.selectMessageByChannelAndDelivery(channelId, deliveryTag).delete
		}

	override def deleteMultiConfIfNoMatchingMsg(olderThanSeconds: Long): Unit =
		Database.forDataSource(writeDs).withSession {
			val formatted = getFormattedDateForSecondsAgo(olderThanSeconds)
			Queries.deleteMultiConfIfNoMsg(formatted).execute()
		}

	override def loadLockedMessages(limit: Int): List[Message] = {
		Database.forDataSource(writeDs).withSession {
			Queries.selectLockedMessages(limit).list()
		}
	}


	override def loadConfirmationByChannels(channelIds: List[String]): List[MessageConfirmation] = {
		if (channelIds.isEmpty) List()
		else {
			Database.forDataSource(writeDs).withSession {
				Queries.selectConfByChannelIds(channelIds).list()
			}
		}
	}

	override def deleteMessage(id: Long): Unit = {
		Database.forDataSource(writeDs).withSession {
			Queries.deleteMessageById(id).execute()
		}
	}

	override def deleteConfirmation(id: Long): Unit = {
		Database.forDataSource(writeDs).withSession {
			Queries.deleteConfById(id).execute()
		}
	}

	override def deleteMatchingMessagesAndSingleConfirms(): Unit = {
		Database.forDataSource(writeDs).withSession {
			Queries.deleteMatchingMessagesAndSingleConfirms().execute()
		}
	}

	override def lockRowsOlderThan(olderThanSeconds: Long, lockTimeOutAfterSeconds: Long, limit: Int): Unit = {
		Database.forDataSource(writeDs).withSession {
			Queries.lockRowsOlderThan(
				getFormattedDateForSecondsAgo(olderThanSeconds),
				getFormattedDateForSecondsAgo(0),
				getFormattedDateForSecondsAgo(lockTimeOutAfterSeconds),
				limit
			).execute()
		}
	}

	override def deleteSingleConfIfNoMatchingMsg(olderThanSeconds: Long): Unit = {
		Database.forDataSource(writeDs).withSession {
			Queries.deleteSingleConfIfNoMatchingMsg(getFormattedDateForSecondsAgo(olderThanSeconds)).execute()
		}
	}

	override def deleteMessagesWithMatchingMultiConfirms(): Unit = {
		Database.forDataSource(writeDs).withSession {
			Queries.deleteMessagesWithMatchingMultiConfirms().execute()
		}
	}

	private def getFormattedDateForSecondsAgo(seconds: Long): String = {
		val date = new Date(System.currentTimeMillis() - seconds * 1000)
		df.format(date)
	}
}
