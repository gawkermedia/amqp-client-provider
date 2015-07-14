package com.kinja.amqp.persistence

import javax.sql.DataSource
import org.h2.jdbcx.JdbcDataSource
import org.specs2.mock.Mockito

import scala.util.Random

trait H2Database {

	final lazy val h2ds: DataSource = {
		val randomName = "db-" + Random.alphanumeric.take(10).mkString
		val url = "jdbc:h2:mem:" + randomName + ";DATABASE_TO_UPPER=false;TRACE_LEVEL_SYSTEM_OUT=0;MODE=MySQL"
		val ds = new JdbcDataSource()
		ds.setURL(url + ";DB_CLOSE_DELAY=2")
		ds
	}
}
