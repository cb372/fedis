package com.github.cb372.fedis.db

import com.twitter.finagle.redis.protocol.{MBulkReply, EmptyBulkReply, BulkReply, IntegerReply}
import com.twitter.util.Time
import com.twitter.conversions.time._

/**
 * Author: chris
 * Created: 6/27/12
 */

trait StringsOps { this: DbCommon =>

  def append(key: String, suffix: Array[Byte]) = pool {
    state.update { m =>
      m get(key) match {
        case Some(Entry(RString(value), expiry)) => {
          val newBytes = value ++ suffix
          val newValue = Entry(RString(newBytes), expiry) // copy expiry
          val updated = m + (key -> newValue)
          updateAndReply(updated, IntegerReply(newBytes.length))
        }
        case Some(_) => noUpdate(Replies.errWrongType)
        case None => {
          val updated = m + (key -> Entry(RString(suffix))) // no expiry
          updateAndReply(updated, IntegerReply(suffix.length))
        }
      }
    }
  }

  def decr(key: String) = incrBy(key, -1)

  def decrBy(key: String, amount: Int) = incrBy(key, -amount)

  def get(key: String) = pool {
    state.read { m =>
      m get(key) match {
        case Some(Entry(RString(value), _)) => BulkReply(value)
        case Some(_) => Replies.errWrongType
        case None => EmptyBulkReply()
      }
    }
  }

  def getBit(key: String, offset: Int) = pool {
    state.read { m =>
      m get(key) match {
        case Some(Entry(RString(value), _)) => {
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
        case Some(_) => Replies.errWrongType
        case None => IntegerReply(0)
      }
    }
  }

  def getSet(key: String, newValue: Array[Byte]) = pool {
    state.update { m =>
      m get(key) match {
        case Some(Entry(RString(oldValue), expiry)) => {
          val updated = m + (key -> Entry(RString(newValue), expiry)) // copy expiry
          updateAndReply(updated, BulkReply(oldValue))
        }
        case Some(_) => noUpdate(Replies.errWrongType)
        case None => {
          val updated = m + (key -> Entry(RString(newValue))) // no expiry
          updateAndReply(updated, EmptyBulkReply())
        }
      }
    }
  }

  def incr(key: String) = incrBy(key, 1)

  def incrBy(key: String, amount: Int) = pool {
    state.update { m =>
      m get(key) match {
        case Some(Entry(RString(value), expiry)) => {
          val stringVal = new String(value)
          try {
            val intVal = stringVal.toInt
            val incremented = intVal + amount
            if (overflowCheck(intVal, amount, incremented)) {
              val updated = m + (key -> Entry(RString(String.valueOf(incremented).getBytes), expiry)) // copy expiry
              updateAndReply(updated, IntegerReply(incremented))
            } else
              noUpdate(Replies.errIntOverflow)
          } catch {
            case e: NumberFormatException => {
              noUpdate(Replies.errNotAnInt)
            }
          }
        }
        case Some(_) => noUpdate(Replies.errWrongType)
        case None => {
          // store the specified amount (treat the non-existent value as 0)
          val updated = m + (key -> Entry(RString(Array(amount.toByte)))) // no expiry
          updateAndReply(updated, IntegerReply(amount))
        }
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

  def mget(keys: List[String]) = pool {
    keys match {
      case Nil => Replies.errWrongNumArgs("mget")
      case _ => state.read { m =>
        val values = keys map(m.get(_).collect({case Entry(RString(value), _) => value }) getOrElse DbConstants.nil)
        MBulkReply(values)
      }
    }
  }

  def mset(kv: Map[String, Array[Byte]]) = pool {
    if (kv.isEmpty)
      Replies.errWrongNumArgs("mset")
    else state.update { m =>
      val updated = kv.foldLeft(m){
        case (map, (key, value)) => map + (key -> Entry(RString(value))) // no expiry (clear any existing expiry)
      }
      updateAndReply(updated, Replies.ok)
    }
  }

  def msetNx(kv: Map[String, Array[Byte]]) = pool {
    if (kv.isEmpty)
      Replies.errWrongNumArgs("msetnx")
    else state.update { m =>
      val existing = kv.keys.count(m.contains(_))
      if (existing == 0) {
        val updated = kv.foldLeft(m){
          case (map, (key, value)) => map + (key -> Entry(RString(value))) // no expiry (clear any existing expiry)
        }
        updateAndReply(updated, IntegerReply(1))
      } else
        noUpdate(IntegerReply(0))
    }
  }



  def set(key: String, value: Array[Byte]) = pool {
    state.update { m =>
      val updated = m + (key -> Entry(RString(value))) // no expiry (clear any existing expiry)
      updateAndReply(updated, Replies.ok)
    }
  }

  // TODO refactor this huge method
  // XXX copy the whole array just to set one bit?!
  def setBit(key: String, offset: Int, value: Int) = pool {
    if (value != 0 && value != 1) {
      Replies.errNotABit
    } else state.update { m =>
      m get(key) match {
        case Some(Entry(RString(array), expiry)) => {
          if (offset >= array.length * 8) {
            // extend the current value
            val bytes = (offset / 8) + 1
            val extendedArray: Array[Byte] = Array.fill(bytes)(DbConstants.zeroByte)
            Array.copy(array, 0, extendedArray, 0, array.length)
            if (value == 1) {
              // set a 1 bit in the last byte
              extendedArray(offset / 8) = (1 << (7 - (offset % 8))).toByte
            }
            val updated = m + (key -> Entry(RString(extendedArray), expiry)) // copy expiry
            updateAndReply(updated, IntegerReply(0))
          } else {
            val oldByte: Byte = array(offset / 8)
            val bitOffset: Int = 7 - (offset % 8)
            val oldBit: Int = (oldByte & ( 1 << bitOffset )) >> bitOffset
            val newByte: Byte = value match {
              case 0 => (oldByte & ~(1 << bitOffset)).toByte
              case 1 => (oldByte & (1 << bitOffset)).toByte
            }
            val newArray: Array[Byte] = new Array[Byte](array.length)
            Array.copy(array, 0, newArray, 0, array.length)
            newArray(offset / 8) = newByte
            val updated = m + (key -> Entry(RString(newArray), expiry)) // copy expiry
            updateAndReply(updated, IntegerReply(oldBit))
          }
        }
        case Some(_) => noUpdate(Replies.errWrongType)
        case None => {
          val bytes = (offset / 8) + 1
          val byteArray: Array[Byte] = Array.fill(bytes)(DbConstants.zeroByte)
          if (value == 1) {
            // set a 1 bit in the last byte
            byteArray(offset / 8) = (1 << (7 - (offset % 8))).toByte
          }
          val updated = m + (key -> Entry(RString(byteArray))) // no expiry
          updateAndReply(updated, IntegerReply(0))
        }
      }
    }
  }

  def setEx(key: String, expireAfter: Long, value: Array[Byte]) = pool {
    state.update { m =>
      val expiry = Time.now + expireAfter.seconds
      val updated = m + (key -> Entry(RString(value), Some(expiry))) // set value and expiry
      updateAndReply(updated, Replies.ok)
    }
  }

  def setNx(key: String, value: Array[Byte]) = pool {
    state.update { m =>
      if (m contains(key))
        noUpdate(IntegerReply(0))
      else {
        val updated = m + (key -> Entry(RString(value))) // no expiry
        updateAndReply(updated, IntegerReply(1))
      }
    }
  }

  def strlen(key: String) = pool {
    state.read { m =>
      m get(key) match {
        case Some(Entry(RString(value), _)) => IntegerReply(value.length)
        case Some(_) => Replies.errWrongType
        case None => IntegerReply(0)
      }
    }
  }

}
