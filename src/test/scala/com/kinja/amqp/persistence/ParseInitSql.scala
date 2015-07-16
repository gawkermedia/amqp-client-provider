package com.kinja.amqp.persistence

trait ParseInitSql {

	protected lazy val initSql: List[String] = {
		val filename = "init.sql"
		val isOpt = Option(this.getClass.getClassLoader.getResourceAsStream(filename))
		val is = isOpt.getOrElse(throw new java.io.FileNotFoundException("test/resources/" + filename))
		val source = scala.io.Source.fromInputStream(is)
		val script = source.mkString
		val statements = script.split(';').toList

		type Clean = String => String
		val stripEngine: Clean = s => """ENGINE=\w+""".r.replaceAllIn(s, "")
		val stripCharset: Clean = s => """(DEFAULT\s+)?CHARSET=\w+""".r.replaceAllIn(s, "")
		val stripBackticks: Clean = s => """`""".r.replaceAllIn(s, "")
		val cleanUpdateTrigger: Clean = s => """\s+ON UPDATE CURRENT_TIMESTAMP""".r.replaceAllIn(s, "")
		val cleanIndexes: Clean = s => """(?m)\s*(PRIMARY\s+)?KEY.+$""".r.replaceAllIn(s, "")
		val finish: Clean = s => """,\s+\)""".r.replaceAllIn(s, ")")

		val clean =
			stripEngine andThen
				stripCharset andThen
				stripBackticks andThen
				cleanUpdateTrigger andThen
				cleanIndexes andThen
				finish

		statements.map(clean)
	}
}
