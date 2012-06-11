package com.github.cb372.fedis.db

/**
 * Author: chris
 * Created: 6/11/12
 */

sealed trait RVal

case class RString(value: Array[Byte]) extends RVal

case class RHash(value: Map[String, Array[Byte]]) extends RVal

// TODO custom data structure to represent Redis sorted set. Sorted list, plus set?
case class RSortedSet(value: Seq[(String, Double)]) extends RVal