package com.github.cb372.fedis.db

import org.jboss.netty.util.CharsetUtil

/**
 * A Redis key.
 *
 * From the Redis docs (http://redis.io/topics/data-types-intro):
 *
 * "Redis keys are binary safe, this means that you can use any binary sequence as a key,
 * from a string like "foo" to the content of a JPEG file. The empty string is also a valid key."
 *
 * Represented as an Array[Byte] with overriden hashCode() and equals() methods
 * to allow equality comparison and hashing.
 *
 * @param array the underlying array
 */
case class RKey(array: Array[Byte]) {
  val hashcode = java.util.Arrays.hashCode(array)

  override def hashCode() = hashcode

  override def equals(other: Any): Boolean = other match {
    case o: RKey => java.util.Arrays.equals(array, o.array)
    case _ => false
  }

  override def toString = "RKey(" + new String(array, CharsetUtil.UTF_8) + ")"
}
