package com.kinja

package object amqp {

	/**
	 * The exception that should be returned when deserializing a message failed.
	 *
	 * @param msg The deserialization error.
	 */
	class DeserializationException(msg: String) extends Exception(msg)

	/**
	 * Defines how to read a message of type `T` from its serialized string representation.
	 *
	 * @tparam T The type of the message.
	 */
	trait Reads[T] {

		/**
		 * Defines how to read a message from its serialized string representation.
		 *
		 * @param s The serialized message.
		 * @return Either the deserialized value of `T` or a `DeserializationException` if deserialization failed.
		 */
		def reads(s: String): Either[DeserializationException, T]
	}

	/**
	 * Defines how to write a message of type `T` into its serialized string representation.
	 */
	trait Writes[T] {

		/**
		 * Defines how to write a message of type `T` into its serialized string representation.
		 *
		 * @param t The message.
		 * @return The serialized representation of the message.
		 */
		def writes(t: T): String
	}

	/**
	 * The default deserializer for string messages.
	 */
	implicit val readsString: Reads[String] = new Reads[String] {
		override def reads(s: String): Either[DeserializationException, String] = Right(s)
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
	private[amqp] def ignore[A](a: A): Unit = ()
}
