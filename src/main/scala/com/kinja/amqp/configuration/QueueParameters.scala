package com.kinja.amqp.configuration

/**
 * queue parameters
 * @param name queue name. if empty, the broker will generate a random name, see Queue.DeclareOk
 * @param passive if true, just check that que queue exists
 * @param durable if true, the queue will be durable i.e. will survive a broker restart
 * @param exclusive if true, the queue can be used by one connection only
 * @param autodelete if true, the queue will be destroyed when it is no longer used
 * @param args additional parameters, such as TTL, ...
 */
final case class QueueParameters(
	name: String,
	passive: Boolean,
	durable: Boolean = true,
	exclusive: Boolean = false,
	autodelete: Boolean = true,
	args: Map[String, AnyRef] = Map.empty[String, AnyRef])
