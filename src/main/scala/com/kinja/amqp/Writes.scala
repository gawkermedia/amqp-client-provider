package com.kinja.amqp

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
