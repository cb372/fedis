package com.github.cb372.fedis
package db

import collection.mutable.{Map => MMap}
import com.twitter.finagle.redis.protocol.{IntegerReply, StatusReply, EmptyBulkReply, BulkReply}
import com.twitter.util.FuturePool

class Db(pool: FuturePool) {
  private val keyValues = MMap[String, Array[Byte]]()


  def get(key: String) = pool {
    keyValues get (key) match {
      case Some(value: Array[Byte]) => BulkReply(value)
      case None => EmptyBulkReply()
    }
  }

  def set(key: String, value: Array[Byte]) = pool {
    keyValues += key -> value
    StatusReply("OK")
  }

  def del(keys: List[_]) = pool {
    val found = keyValues.filterKeys(keys.contains(_))
    found foreach (keyValues -= _._1)
    IntegerReply(found.size)
  }

  def exists(key: String) = pool {
    if (keyValues contains key)
      IntegerReply(1)
    else
      IntegerReply(0)
  }

  def append(key: String, suffix: Array[Byte]) = pool {
    keyValues get (key) match {
      case Some(value: Array[Byte]) => {
        val newValue = value ++ suffix
        keyValues += key -> newValue
        IntegerReply(newValue.length)
      }
      case None => {
        keyValues += key -> suffix
        IntegerReply(suffix.length)
      }
    }
  }
}
