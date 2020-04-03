package com.kinja

import scala.annotation.tailrec

package object amqp {

	/**
	 * The default deserializer for string messages.
	 */
	implicit val readsString: Reads[String] = new Reads[String] {
		override def reads(s: String): Either[DeserializationException, String] =
			Right[DeserializationException, String](s)
	}

	/**
	 * The default serializer for string messages.
	 */
	implicit val writesString: Writes[String] = new Writes[String] {
		override def writes(s: String): String = s
	}

	/**
	 * Ignores the return value of a function. This can be used to work around the
	 * "discarded non-Unit value" compile errors which aims to prevent bugs.
	 */
	private[amqp] def ignore[A](a: A): Unit = { val _ = a; () }

	/**
	 * Extract the first meaningful error message from an exception hierarchy.
	 * @param throwable
	 * @return error message
	 */
	@tailrec
	private[amqp] def extractErrorMessage(throwable: Throwable): String = {
		(Option(throwable.getCause), Option(throwable.getMessage)) match {
			case (_, Some(msg)) => msg
			case (Some(cause), None) => extractErrorMessage(cause)
			case (None, None) => "null"
		}
	}
}
