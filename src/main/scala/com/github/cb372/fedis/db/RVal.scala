package com.github.cb372.fedis.db

import collection.immutable

/**
 * Author: chris
 * Created: 6/11/12
 */

sealed trait RVal

case class RString(bytes: immutable.IndexedSeq[Byte]) extends RVal

object RString {
  def apply(string: String): RString = apply(string.getBytes)

  def apply(bytesArray: Array[Byte]): RString = RString(bytesArray.toIndexedSeq)
}

case class RHash(hash: Map[HashKey, Array[Byte]]) extends RVal

// TODO custom data structure to represent Redis sorted set. Sorted list, plus set?
case class RSortedSet(set: Seq[(String, Double)]) extends RVal

case class HashKey(array: Array[Byte]) {
  val hashcode = java.util.Arrays.hashCode(array)

  override def hashCode() = hashcode

  override def equals(other: Any): Boolean = other match {
    case o: HashKey => java.util.Arrays.equals(array, o.array)
    case _ => false
  }
}

