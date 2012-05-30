package com.github.cb372.fedis
package db

import collection.mutable.{Map => MMap}
import com.twitter.util.FuturePool
import com.twitter.finagle.redis.protocol._

class Db(pool: FuturePool) {
  private val keyValues = MMap[String, Array[Byte]]()

  /*
   * Keys stuff
   */

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

  /*
   * Strings stuff
   */

  def append(key: String, suffix: Array[Byte]) = pool {
    keyValues get(key) match {
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

  def decr(key: String) = incrBy(key, -1)

  def decrBy(key: String, amount: Int) = incrBy(key, -amount)

  def get(key: String) = pool {
    keyValues get(key) match {
      case Some(value: Array[Byte]) => BulkReply(value)
      case None => EmptyBulkReply()
    }
  }

  def incr(key: String) = incrBy(key, 1)

  def incrBy(key: String, amount: Int) = pool {
    keyValues get(key) match {
      case Some(value: Array[Byte]) => {
        val stringVal = new String(value)
        try {
          // TODO over/underflow checking
          val intVal = stringVal.toInt
          val incremented = intVal + amount
          keyValues.put(key, String.valueOf(incremented).getBytes)
          IntegerReply(incremented)
        } catch {
          case e: NumberFormatException => {
            ErrorReply("ERR value is not an integer or out of range")
          }
        }
      }
      case None => {
        // store the specified amount (treat the non-existent value as 0)
        keyValues.put(key, Array(amount.toByte))
        IntegerReply(amount)
      }
    }
  }

  def set(key: String, value: Array[Byte]) = pool {
    keyValues += key -> value
    StatusReply("OK")
  }

  def strlen(key: String) = pool {
    keyValues get(key) match {
      case Some(value: Array[Byte]) => IntegerReply(value.length)
      case None => IntegerReply(0)
    }
  }
}
