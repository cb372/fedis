package com.github.cb372.fedis
package db

import collection.mutable.{Map => MMap}
import com.twitter.util.FuturePool
import com.twitter.finagle.redis.protocol._

object Db {
  private val zeroByte = 0.toByte
}

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

  def getBit(key: String, offset: Int) = pool {
    keyValues get(key) match {
      case Some(value: Array[Byte]) => {
        if (offset >= value.length * 8)
          // offset is longer than string
          IntegerReply(0)
        else {
          val byte: Byte = value(offset / 8)
          val bitOffset: Int = 7 - (offset % 8)
          val theBit: Int = (byte & ( 1 << bitOffset )) >> bitOffset
          IntegerReply(theBit)
        }
      }
      case None => IntegerReply(0)
    }
  }

  def incr(key: String) = incrBy(key, 1)

  def incrBy(key: String, amount: Int) = pool {
    keyValues get(key) match {
      case Some(value: Array[Byte]) => {
        val stringVal = new String(value)
        try {
          val intVal = stringVal.toInt
          val incremented = intVal + amount
          if (overflowCheck(intVal, amount, incremented)) {
            keyValues.put(key, String.valueOf(incremented).getBytes)
            IntegerReply(incremented)
          } else
            ErrorReply("ERR increment or decrement would overflow")
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

  /*
   * Overflow checking, returns true if the addition did NOT cause an overflow
   */
  private def overflowCheck(before: Int, added: Int, after: Int): Boolean = {
    if (added >= 0)
      after > before
    else
      after < before
  }

  def set(key: String, value: Array[Byte]) = pool {
    keyValues += key -> value
    StatusReply("OK")
  }

  def setBit(key: String, offset: Int, value: Int) = pool {
    if (value != 0 && value != 1) {
      ErrorReply("ERR bit is not an integer or out of range")
    } else {
      keyValues get(key) match {
        case Some(array: Array[Byte]) => {
          if (offset >= array.length * 8) {
            // extend the current value
            val bytes = (offset / 8) + 1
            val extendedArray: Array[Byte] = Array.fill(bytes)(Db.zeroByte)
            Array.copy(array, 0, extendedArray, 0, array.length)
            if (value == 1) {
              // set a 1 bit in the last byte
              extendedArray(offset / 8) = (1 << (7 - (offset % 8))).toByte
            }
            keyValues.put(key, extendedArray)
            IntegerReply(0)
          } else {
            val oldByte: Byte = array(offset / 8)
            val bitOffset: Int = 7 - (offset % 8)
            val oldBit: Int = (oldByte & ( 1 << bitOffset )) >> bitOffset
            val newByte: Byte = value match {
              case 0 => (oldByte & ~(1 << bitOffset)).toByte
              case 1 => (oldByte & (1 << bitOffset)).toByte
            }
            array(offset / 8) = newByte
            IntegerReply(oldBit)
          }
        }
        case None => {
          val bytes = (offset / 8) + 1
          val byteArray: Array[Byte] = Array.fill(bytes)(Db.zeroByte)
          if (value == 1) {
            // set a 1 bit in the last byte
            byteArray(offset / 8) = (1 << (7 - (offset % 8))).toByte
          }
          keyValues.put(key, byteArray)
          IntegerReply(0)
        }
      }
    }
  }

  def strlen(key: String) = pool {
    keyValues get(key) match {
      case Some(value: Array[Byte]) => IntegerReply(value.length)
      case None => IntegerReply(0)
    }
  }
}
