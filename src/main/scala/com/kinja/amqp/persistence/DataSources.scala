package com.kinja.amqp.persistence

trait DataSources {

	def writeDs: javax.sql.DataSource

}
