package com.kinja.amqp.persistence

import com.kinja.amqp.TransactionalMessageStore
import com.kinja.amqp.model.Message
import com.kinja.amqp.model.MessageConfirmation

import scala.slick.jdbc.GetResult.GetLong
import scala.slick.jdbc.StaticQuery
import scala.slick.jdbc.GetResult
import scala.slick.lifted.ColumnBase
import slick.driver.ExtendedProfile
import scala.slick.lifted.BaseTypeMapper

import java.text.SimpleDateFormat
import java.util.Date

abstract class MySqlMessageStore extends MessageStore {

	this: ExtendedProfile =>
	import simple._

	import StaticQuery.interpolation

	def writeDs: javax.sql.DataSource

	implicit val getDateResult = GetResult(r => new Date(GetLong(r)))
	implicit val getMessageConf = GetResult(r => MessageConfirmation(r.<<, r.<<, r.<<, r.<<, r.<<))
	implicit val getMessage = GetResult(r => Message(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))

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

		def * : ColumnBase[Message] =
			id.? ~ routingKey ~ exchangeName ~ message ~ channelId ~ deliveryTag ~ createdTime <> (Message.apply _, Message.unapply _)

		def autoInc =
			id.? ~ routingKey ~ exchangeName ~ message ~ channelId ~ deliveryTag ~ createdTime <> (Message.apply _, Message.unapply _) returning id
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

		def commit() =
			sqlu"""
				COMMIT
			"""

		def startTransaction() =
			sqlu"""
				START TRANSACTION
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
				FROM rabbit_confirmations
				WHERE `createdTime` < $formattedTime
					AND `multiple` = true
					AND `channelId` NOT IN (
						SELECT chid FROM (
							SELECT DISTINCT m.channelId AS chid
							FROM rabbit_messages m
								JOIN rabbit_confirmations c
								ON m.channelId = c.channelId
							WHERE m.deliveryTag <= c.deliveryTag
								AND c.multiple = true)
						AS c)
			"""

		def selectMessageCreatedBefore(formattedTime: String, exchangeName: String, limit: Int) =
			sql"""
				SELECT *
			 		FROM rabbit_messages
					WHERE `exchangeName` = $exchangeName
						AND `createdTime` < $formattedTime
		 		LIMIT $limit
		 		FOR UPDATE
			""".as[Message]

		def selectConfByChannelIds(ids: List[String]) = {
			val whereClause = s"""WHERE `channelId` IN (${ids.map("'" + _ + "'").mkString(",")})"""
			sql"""
				SELECT *
			 		FROM rabbit_confirmations
					#$whereClause
			""".as[MessageConfirmation]
		}

		def selectConfByChannelAndDelivery(channelId: String, deliveryTag: Long) = for {
			c <- MessageConfirmationTable
			if c.channelId === channelId && c.deliveryTag === deliveryTag
		} yield c
	}

	override def saveMessage(msg: Message): Unit = {
		Database.forDataSource(writeDs).withSession {
			import Database.threadLocalSession
			MessageTable.autoInc.insert(msg)
		}
	}

	override def saveConfirmation(conf: MessageConfirmation): Unit = {
		Database.forDataSource(writeDs).withSession {
			import Database.threadLocalSession
			MessageConfirmationTable.autoInc.insert(conf)
		}
	}

	override def deleteMessageUponConfirm(channelId: String, deliveryTag: Long): Int =
		Database.forDataSource(writeDs).withSession {
			import Database.threadLocalSession
			Queries.selectConfByChannelAndDelivery(channelId, deliveryTag).delete
		}

	override def deleteMultiConfIfNoMatchingMsg(olderThan: Long): Unit =
		Database.forDataSource(writeDs).withSession {
			import Database.threadLocalSession
			val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
			val formatted = df.format(new Date(olderThan))
			Queries.deleteMultiConfIfNoMsg(formatted).execute()
		}

	def createTransactionalStore: TransactionalMessageRepo = {
		implicit val session = Database.forDataSource(writeDs).createSession()
		new TransactionalMessageRepo()
	}

	class TransactionalMessageRepo(implicit session: Session) extends TransactionalMessageStore {

		override def start = Queries.startTransaction().execute()
		override def commit = {
			try {
				Queries.commit().execute()
			} finally { session.close() }
		}

		override def loadMessageOlderThan(time: Long, exchangeName: String, limit: Int): List[Message] = {
			val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
			val formatted = df.format(new Date(time))
			Queries.selectMessageCreatedBefore(formatted, exchangeName, limit).list()
		}

		override def loadConfirmationByChannels(channelIds: List[String]): List[MessageConfirmation] = {
			if (channelIds.isEmpty) List()
			else {
				val confs = Queries.selectConfByChannelIds(channelIds).list()
				confs
			}
		}

		override def deleteMessage(id: Long): Unit = {
			Queries.deleteMessageById(id).execute()
		}

		override def deleteConfirmation(id: Long): Unit =
			Queries.deleteConfById(id).execute()
	}

}
