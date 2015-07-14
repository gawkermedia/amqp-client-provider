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

	val readDs: javax.sql.DataSource
	val writeDs: javax.sql.DataSource

	protected def onRead[T](block: Connection => T): T = {
		val conn = readDs.getConnection()
		try {
			block(conn)
		} finally {
			if (conn != null) conn.close()
		}
	}

	protected def onWrite[T](block: Connection => T): T = {
		val conn = writeDs.getConnection()
		try {
			block(conn)
		} finally {
			if (conn != null) conn.close()
		}
	}

	protected def prepare[T](query: String)(block: PreparedStatement => T)(implicit conn: Connection): T = {
		val stmt = conn.prepareStatement(query)
		try {
			block(stmt)
		} finally {
			if (stmt != null) stmt.close()
		}
	}

	protected trait GetResult[T] extends Function1[ResultSet, T]

	protected object GetResult {
		def apply[T](fn: ResultSet => T): GetResult[T] = new GetResult[T] {
			def apply(r: ResultSet): T = fn(r)
		}
	}

	protected trait SetResult[T] extends Function2[PreparedStatement, T, Unit]

	protected object SetResult {
		def apply[T](fn: (PreparedStatement, T) => Unit): SetResult[T] = new SetResult[T] {
			def apply(stmt: PreparedStatement, t: T): Unit = fn(stmt, t)
		}
	}

	protected implicit class QueryExecutor(stmt: PreparedStatement) {

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

	protected def questionmarks(fields: List[String]): String = fields.map(_ => "?").mkString(",")
}
