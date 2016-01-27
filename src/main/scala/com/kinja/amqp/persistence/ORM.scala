package com.kinja.amqp.persistence

import java.sql.{ Date, Timestamp }
import java.text.SimpleDateFormat

import com.kinja.amqp.model.{ Message, MessageConfirmation }

import scala.concurrent.Future
import scala.collection.mutable.ListBuffer

import java.sql.{ Connection, PreparedStatement, ResultSet }

/**
 * Provides very basic ORM functions.
 */
trait ORM {

	val getReadConnection: () => Connection
	val getWriteConnection: () => Connection

	private[persistence] class ColumnBase(val columns: List[Column]) {
		def ~(other: Column): ColumnBase = new ColumnBase(columns :+ other)
	}

	private[persistence] class Column(val name: String) {
		def ~(other: Column): ColumnBase = new ColumnBase(List(this, other))
	}

	private[persistence] abstract class Table[T](name: String) {
		def column(name: String): Column = new Column(name)

		val * : ColumnBase

		lazy val columnNames: List[String] = *.columns.map(_.name)

		lazy val insertStatement =
			s"""
				INSERT INTO $name (${columnNames.mkString(",")})
				VALUES (${questionmarks(columnNames)})
			"""
	}

	private[persistence] def onRead[T](block: Connection => T): T = {
		val conn = getReadConnection()
		try {
			block(conn)
		} finally {
			if (Option(conn).isDefined) conn.close()
		}
	}

	private[persistence] def onWrite[T](block: Connection => T): T = {
		val conn = getWriteConnection()
		try {
			block(conn)
		} finally {
			if (Option(conn).isDefined) conn.close()
		}
	}

	private[persistence] def prepare[T](query: String)(block: PreparedStatement => T)(implicit conn: Connection): T = {
		val stmt = conn.prepareStatement(query)
		try {
			block(stmt)
		} finally {
			if (Option(stmt).isDefined) stmt.close()
		}
	}

	private[persistence] trait Reads[T] extends Function2[ResultSet, String, T]

	private[persistence] implicit val stringReads: Reads[String] = new Reads[String] {
		def apply(r: ResultSet, column: String): String = r.getString(column)
	}

	private[persistence] implicit val longReads: Reads[Long] = new Reads[Long] {
		def apply(r: ResultSet, column: String): Long = r.getLong(column)
	}

	private[persistence] implicit val intReads: Reads[Int] = new Reads[Int] {
		def apply(r: ResultSet, column: String): Int = r.getInt(column)
	}

	private[persistence] implicit val booleanReads: Reads[Boolean] = new Reads[Boolean] {
		def apply(r: ResultSet, column: String): Boolean = r.getBoolean(column)
	}

	private[persistence] implicit val timestampReads: Reads[Timestamp] = new Reads[Timestamp] {
		def apply(r: ResultSet, column: String): Timestamp = r.getTimestamp(column)
	}

	private[persistence] implicit def optionReads[T: Reads]: Reads[Option[T]] = new Reads[Option[T]] {
		def apply(r: ResultSet, column: String): Option[T] = {
			val value = implicitly[Reads[T]].apply(r, column)
			if (r.wasNull) None else Option(value)
		}
	}

	private[persistence] implicit class ResultSetReader(val r: ResultSet) {
		def read[T: Reads](column: String): T = implicitly[Reads[T]].apply(r, column)
	}

	private[persistence] trait GetResult[T] extends Function1[ResultSet, T]

	private[persistence] object GetResult {
		def apply[T](fn: ResultSet => T): GetResult[T] = new GetResult[T] {
			def apply(r: ResultSet): T = fn(r)
		}
	}

	private[persistence] trait SetResult[T] extends Function2[PreparedStatement, T, Unit]

	private[persistence] object SetResult {
		def apply[T](fn: (PreparedStatement, T) => Unit): SetResult[T] = new SetResult[T] {
			def apply(stmt: PreparedStatement, t: T): Unit = fn(stmt, t)
		}
	}

	private[persistence] implicit class QueryExecutor(stmt: PreparedStatement) {

		def list[T: GetResult]: List[T] = {
			val getResult = implicitly[GetResult[T]]
			val buffer = ListBuffer[T]()
			val rs = stmt.executeQuery
			while (rs.next) {
				buffer += getResult(rs)
			}
			buffer.toList
		}

		def insert[T: SetResult](t: T): Int = {
			val setResult = implicitly[SetResult[T]]
			setResult.apply(stmt, t)
			stmt.executeUpdate
		}

		@SuppressWarnings(Array("org.brianmckenna.wartremover.warts.Var"))
		def insertAll[T: SetResult](ts: Traversable[T])(implicit conn: Connection): Unit = {
			val setResult = implicitly[SetResult[T]]
			val autoCommit = conn.getAutoCommit()
			if (autoCommit) {
				conn.setAutoCommit(false)
			}
			var i = 0
			ts foreach { t =>
				setResult.apply(stmt, t)

				stmt.addBatch()
				i = i + 1

				if (i % 1000 == 0 || i == ts.size) {
					stmt.executeBatch() // execute every 1000 items
				}
			}
			conn.commit()
			if (autoCommit) {
				conn.setAutoCommit(true)
			}
		}
	}

	private[persistence] def questionmarks(fields: List[String]): String = fields.map(_ => "?").mkString(",")
}
