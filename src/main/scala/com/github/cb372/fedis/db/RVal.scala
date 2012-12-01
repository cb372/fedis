package com.github.cb372.fedis.db

import collection.immutable

/**
 * Author: chris
 * Created: 6/11/12
 */

/**
 * A value in a Redis DB.
 */
sealed trait RVal

/**
 * A Redis string. Represented as an immutable list of bytes.
 * @param bytes the raw bytes
 */
case class RString(bytes: immutable.IndexedSeq[Byte]) extends RVal

object RString {
  def apply(string: String): RString = apply(string.getBytes)

  def apply(bytesArray: Array[Byte]): RString = RString(bytesArray.toIndexedSeq)
}

/**
 * A Redis hash, represented as a Map of Array[Byte] -> Array[Byte]
 * @param hash the underlying map
 */
case class RHash(hash: Map[HashKey, Array[Byte]]) extends RVal

// TODO custom data structure to represent Redis sorted set. Sorted list, plus set?
case class RSortedSet(set: Seq[(String, Double)]) extends RVal

/**
 * An Array[Byte] with overriden hashCode() and equals() methods.
 * @param array the underlying array
 */
case class HashKey(array: Array[Byte]) {
  val hashcode = java.util.Arrays.hashCode(array)

  override def hashCode() = hashcode

  override def equals(other: Any): Boolean = other match {
    case o: HashKey => java.util.Arrays.equals(array, o.array)
    case _ => false
  }
}

