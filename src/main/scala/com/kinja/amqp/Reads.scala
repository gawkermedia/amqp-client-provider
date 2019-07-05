package com.kinja.amqp

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
