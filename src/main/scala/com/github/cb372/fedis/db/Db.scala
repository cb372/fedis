package com.github.cb372.fedis
package db

import com.twitter.finagle.redis.protocol._
import com.twitter.util.{Time, FuturePool}
import com.twitter.conversions.time._

trait KeyValueStore {
  def iterator: Iterator[(String, Entry)]

  def remove(key: String)
}

object Db {
  private val zeroByte = 0.toByte
  private def nil = new Array[Byte](0)
}

object Replies {
  val ok = StatusReply("OK")

  val errWrongType = ErrorReply("ERR Operation against a key holding the wrong kind of value")
  val errIntOverflow = ErrorReply("ERR increment or decrement would overflow")
  val errNotAnInt = ErrorReply("ERR value is not an integer or out of range")
  val errNotABit = ErrorReply("ERR bit is not an integer or out of range")

  def errWrongNumArgs(cmdName: String) =
    ErrorReply("ERR wrong number of arguments for '%s' command".format(cmdName.toLowerCase))

}

class Db(pool: FuturePool) extends KeyValueStore {
  private val state = new AtomicRefDbState

  def iterator = state.read(_.iterator)

  def remove(key: String) {
    state.update {m => (Some(m - key), Unit)}
  }

  /*
   * Keys stuff
   */

  def del(keys: List[String]) = pool {
    state.update { m =>
      val found = keys.filter(m.contains(_))
      val updated = found.foldLeft(m)(_ - _)
      updateAndReply(updated, IntegerReply(found.size))
    }
  }

  def exists(key: String) = pool {
    state.read { m =>
      if (m contains key)
        IntegerReply(1)
      else
        IntegerReply(0)
    }
  }

  def expire(key: String, after: Long) = pool {
    state.update { m =>
      m get(key) match {
        case Some(Entry(value, _)) => {
          val newExpiry = Time.now + after.seconds
          val updated = m + (key -> Entry(value, Some(newExpiry))) // set to expire N seconds from now
          updateAndReply(updated, IntegerReply(1))
        }
        case None => noUpdate(IntegerReply(0))
      }
    }
  }

  def expireAt(key: String, timestamp: Time) = pool {
    state.update { m =>
      m get(key) match {
        case Some(Entry(value, _)) => {
          val updated = m + (key -> Entry(value, Some(timestamp))) // set to expire at specified time (even if it is in the past)
          updateAndReply(updated, IntegerReply(1))
        }
        case None => noUpdate(IntegerReply(0))
      }
    }
  }

  def persist(key: String) = pool {
    state.update { m =>
      m get(key) flatMap {
        case Entry(value, expiry) => {
          expiry map { _ =>
           val updated = m + (key -> Entry(value, None)) // remove timeout
           updateAndReply(updated, IntegerReply(1))
          }
        }
      } getOrElse noUpdate(IntegerReply(0)) // entry does not exist, or does not have a timeout
    }
  }

  private val rnd = new util.Random

  def randomKey() = pool {
    state.read { m =>
      if (m.isEmpty)
        EmptyBulkReply()
      else {
        val keys = m.keys
        // Note: this is O(n) in the size of the map!
        val randomKey = keys.drop(rnd.nextInt(keys.size)).head
        BulkReply(randomKey.getBytes("UTF-8"))
      }
    }
  }

  def ttl(key: String) = pool {
    state.read { m =>
      m get(key) flatMap {
        case Entry(_, expiry) => {
          expiry map { e =>
            val ttl = (e - Time.now).inSeconds
            IntegerReply(ttl)
          }
        }
      } getOrElse IntegerReply(-1) // entry does not exist, or does not have a timeout
    }
  }

  /*
   * Strings stuff
   */

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
        val values = keys map(m.get(_).collect({case Entry(RString(value), _) => value }) getOrElse Db.nil)
        MBulkReply(values)
      }
    }
  }

  def mset(kv: Map[String, Array[Byte]]) = pool {
    if (kv isEmpty)
      Replies.errWrongNumArgs("mset")
    else state.update { m =>
      val updated = kv.foldLeft(m){
        case (map, (key, value)) => map + (key -> Entry(RString(value))) // no expiry (clear any existing expiry)
      }
      updateAndReply(updated, Replies.ok)
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
            val extendedArray: Array[Byte] = Array.fill(bytes)(Db.zeroByte)
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
          val byteArray: Array[Byte] = Array.fill(bytes)(Db.zeroByte)
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

  /*
   * Hashes stuff
   *
   * Note: Redis hashes support arbitrary byte arrays for both
   * field keys and values, but finagle-redis is inconsistent
   * in their typing of field keys.
   * They use String for some commands and Array[Byte] for others.
   */

  def hdel(key: String, fields: Seq[String]) = pool {
    state.update { m =>
      m get(key) match {
        case Some(Entry(RHash(hash), expiry)) => {
          val fieldKeys = fields.map(s => HashKey(s.getBytes))
          val deleteCount = fieldKeys.count(hash.contains(_))
          if (deleteCount > 0) {
            val newHash = hash -- fieldKeys
            val updated = m + (key -> Entry(RHash(newHash), expiry)) // copy expiry
            updateAndReply(updated, IntegerReply(deleteCount))
          } else
            noUpdate(IntegerReply(0))
        }
        case Some(_) => noUpdate(Replies.errWrongType)
        case None => noUpdate(IntegerReply(0))
      }
    }
  }

  def hget(key: String, field: Array[Byte]) = pool {
    state.read { m =>
      m get(key) match {
        case Some(Entry(RHash(hash), _)) =>
          hash.get(HashKey(field)).map(BulkReply(_)) getOrElse(EmptyBulkReply())
        case Some(_) => Replies.errWrongType
        case None => EmptyBulkReply()
      }
    }
  }

  def hgetAll(key: String) = pool {
    state.read { m =>
      m get(key) match {
        case Some(Entry(RHash(hash), _)) => {
          val list = hash.flatMap({case (k, v) => List(k.array, v)}).toList
          MBulkReply(list)
        }
        case Some(_) => Replies.errWrongType
        case None => EmptyMBulkReply()
      }
    }
  }

  /*
   * D'oh! finagle-redis doesn't provide a protocol class for this command.
   */
  def hlen(key: String) = pool {
    state.read { m =>
      m get(key) match {
        case Some(Entry(RHash(hash), _)) => IntegerReply(hash.size)
        case Some(_) => Replies.errWrongType
        case None => IntegerReply(0)
      }
    }
  }

  def hset(key: String, field: Array[Byte], value: Array[Byte]) = pool {
    val hashKey = HashKey(field)
    state.update { m =>
      m get(key) match {
        case Some(Entry(RHash(hash), expiry)) => {
          val alreadyContainsField = hash.contains(hashKey)
          val newHash = hash + (hashKey -> value)
          val reply =
            if (alreadyContainsField)
              IntegerReply(0)
            else
              IntegerReply(1)
          val updated = m + (key -> Entry(RHash(newHash), expiry)) // copy expiry
          updateAndReply(updated, reply)
        }
        case Some(_) => noUpdate(Replies.errWrongType)
        case None => {
          val updated = m + (key -> Entry(RHash(Map(hashKey -> value))))
          updateAndReply(updated, IntegerReply(1))
        }
      }
    }
  }


  private def noUpdate[R <: Reply](reply: R): (Option[Map[String, Entry]], R) = (None, reply)

  private def updateAndReply[R <: Reply](updatedMap: Map[String, Entry], reply: R): (Option[Map[String, Entry]], R) =
    (Some(updatedMap), reply)
}
