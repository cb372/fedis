package com.github.cb372.fedis.db

import com.twitter.finagle.redis.protocol._

/**
 * Author: chris
 * Created: 6/27/12
 */

trait HashesOps { this: DbCommon =>

  /*
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

}
