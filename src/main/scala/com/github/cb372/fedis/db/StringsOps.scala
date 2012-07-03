package com.github.cb372.fedis.db

import com.twitter.finagle.redis.protocol.{MBulkReply, EmptyBulkReply, BulkReply, IntegerReply}
import com.twitter.util.Time
import com.twitter.conversions.time._
import collection.immutable.IndexedSeq

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
        case Some(Entry(RString(value), _)) => BulkReply(value.toArray)
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

  def getRange(key: String, start: Int, end: Int) = pool {
    state.read { m =>
      m get(key) match {
        case Some(Entry(RString(value), _)) => {
          val from = positiveIndex(start, value.length)
          val to = positiveIndex(end, value.length)
          val substr = value.slice(from, to + 1)
          if (substr.isEmpty)
            EmptyBulkReply()
          else
            BulkReply(substr.toArray)
        }
        case Some(_) => Replies.errWrongType
        case None => EmptyBulkReply()
      }
    }
  }

  private def positiveIndex(index: Int, len: Int): Int =
    if (index < 0)
      (len + index)
    else
      index


  def getSet(key: String, newValue: Array[Byte]) = pool {
    state.update { m =>
      m get(key) match {
        case Some(Entry(RString(oldValue), expiry)) => {
          val updated = m + (key -> Entry(RString(newValue), expiry)) // copy expiry
          updateAndReply(updated, BulkReply(oldValue.toArray))
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
          val stringVal = new String(value.toArray)
          try {
            val intVal = stringVal.toInt
            val incremented = intVal + amount
            if (overflowCheck(intVal, amount, incremented)) {
              val updated = m + (key -> Entry(RString(String.valueOf(incremented)), expiry)) // copy expiry
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
        val values = keys map(m.get(_).collect({case Entry(RString(value), _) => value.toArray }) getOrElse DbConstants.nil)
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
      val updated = m + (key -> Entry(RString((value)))) // no expiry (clear any existing expiry)
      updateAndReply(updated, Replies.ok)
    }
  }

  def setBit(key: String, offset: Int, value: Int) = pool {
    if (value != 0 && value != 1) {
      Replies.errNotABit
    } else state.update { m =>
      m get(key) match {
        case Some(Entry(RString(oldValue), expiry)) => {
          val (newValue, oldBit) = doSetBit(oldValue, offset, value)
          val updated = m + (key -> Entry(RString(newValue), expiry)) // copy expiry
          updateAndReply(updated, IntegerReply(oldBit))
        }
        case Some(_) => noUpdate(Replies.errWrongType)
        case None => {
          val numBytes = (offset / 8) + 1
          var seq = IndexedSeq.fill(numBytes)(DbConstants.zeroByte)
          if (value == 1) {
            // set a 1 bit in the last byte
            seq = seq.updated(offset / 8, (1 << (7 - (offset % 8))).toByte)
          }
          val updated = m + (key -> Entry(RString(seq))) // no expiry
          updateAndReply(updated, IntegerReply(0))
        }
      }
    }
  }

  private def doSetBit(
                        seq: IndexedSeq[Byte],
                        offset: Int,
                        bit: Int
                        ): (IndexedSeq[Byte], Int) = {
    // pad the existing vector if necessary
    val padded =
      if (offset >= seq.length * 8)
        seq.padTo((offset / 8) + 1, DbConstants.zeroByte)
      else
        seq

    // find the appropriate byte
    val oldByte: Byte = padded(offset / 8)
    // find the appropriate bit in that byte
    val bitOffset: Int = 7 - (offset % 8)
    // get the old value of the bit
    val oldBit: Int = (oldByte & ( 1 << bitOffset )) >> bitOffset
    // update the byte
    val newByte: Byte = bit match {
      case 0 => (oldByte & ~(1 << bitOffset)).toByte
      case 1 => (oldByte | (1 << bitOffset)).toByte
    }
    // update the vector
    val newSeq = padded.updated(offset / 8, newByte)

    (newSeq, oldBit)
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

  def setRange(key: String, offset: Int, substr: Array[Byte]) = pool {
    if (offset < 0)
      Replies.errOffsetOutOfRange
    else
      state.update { m =>
        m get(key) match {
          case Some(Entry(RString(value), expiry)) => {
            val newLen = (offset + substr.length) max value.length
            val padded =
              if (newLen > value.length)
                value.padTo(newLen, DbConstants.zeroByte)
              else
                value
            val newValue: IndexedSeq[Byte] = substr.foldLeft((padded, offset)) {
              case ((string, i), b) => (string.updated(i, b), i + 1)
            }._1
            val updated = m + (key -> Entry(RString(newValue), expiry)) // copy expiry
            updateAndReply(updated, IntegerReply(newLen))
          }
          case Some(_) => noUpdate(Replies.errWrongType)
          case None => {
            val zeroPadded = IndexedSeq.fill(offset)(DbConstants.zeroByte) ++ substr.toIndexedSeq
            val updated = m + (key -> Entry(RString(zeroPadded))) // no expiry
            updateAndReply(updated, IntegerReply(zeroPadded.length))
          }
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
