package com.github.cb372.fedis.db

/**
 * Author: chris
 * Created: 6/11/12
 */

sealed trait RVal

case class RString(bytes: Array[Byte]) extends RVal

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

