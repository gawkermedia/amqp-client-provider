package com.kinja.amqp

/**
 * The exception that should be returned when deserializing a message failed.
 *
 * @param msg The deserialization error.
 */
class DeserializationException(msg: String) extends Exception(msg)
