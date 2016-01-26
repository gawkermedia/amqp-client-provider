package com.kinja

package object amqp {

	class ParsingException(msg: String) extends Exception(msg)

	trait Reads[T] {
		def reads(s: String): Either[ParsingException, T]
	}

	trait Writes[T] {
		def writes(t: T): String
	}

	implicit val readsString: Reads[String] = new Reads[String] {
		override def reads(s: String): Either[ParsingException, String] = Right(s)
	}

	implicit val writesString: Writes[String] = new Writes[String] {
		override def writes(s: String): String = s
	}
}
