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
case class RHash(hash: Map[RKey, Array[Byte]]) extends RVal

case class RList(list: List[Array[Byte]]) extends RVal

case class RSet(set: Set[RKey]) extends RVal

// TODO custom data structure to represent Redis sorted set. Sorted list, plus set?
case class RSortedSet(set: Seq[(String, Double)]) extends RVal


