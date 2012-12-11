package com.github.cb372.fedis.db

import com.twitter.finagle.redis.protocol._

/**
 * Author: chris
 * Created: 6/27/12
 */

trait HashesOps extends ReplyFactory {
  this: DbCommon =>

  def hdel(key: RKey, fields: Seq[RKey]) = pool {
    state.update { m =>
      m get(key) match {
        case Some(Entry(RHash(hash), expiry)) => {
          val deleteCount = fields.count(hash.contains(_))
          if (deleteCount > 0) {
            val newHash = hash -- fields
            val updated =
              if (newHash.isEmpty)
                // delete the key
                m - key
              else
                m + (key -> Entry(RHash(newHash), expiry)) // copy expiry
            updateAndReply(updated, IntegerReply(deleteCount))
          } else
            noUpdate(IntegerReply(0))
        }
        case Some(_) => noUpdate(Replies.errWrongType)
        case None => noUpdate(IntegerReply(0))
      }
    }
  }

  def hget(key: RKey, field: RKey) = pool {
    state.read { m =>
      m get(key) match {
        case Some(Entry(RHash(hash), _)) =>
          hash.get(field).map(bulkReply(_)) getOrElse(EmptyBulkReply())
        case Some(_) => Replies.errWrongType
        case None => EmptyBulkReply()
      }
    }
  }

  def hgetAll(key: RKey) = pool {
    state.read { m =>
      m get(key) match {
        case Some(Entry(RHash(hash), _)) => {
          val keyValuePairs = hash.flatMap {
            case (k, v) => List(bulkReply(k.array), bulkReply(v))
          }.toList
          MBulkReply(keyValuePairs)
        }
        case Some(_) => Replies.errWrongType
        case None => EmptyMBulkReply()
      }
    }
  }

  def hkeys(key: RKey) = pool {
    state.read { m =>
      m get(key) match {
        case Some(Entry(RHash(hash), _)) => {
          val keys = hash.keys.map(k => bulkReply(k.array)).toList
          MBulkReply(keys)
        }
        case Some(_) => Replies.errWrongType
        case None => EmptyMBulkReply()
      }
    }
  }

  /*
   * D'oh! finagle-redis doesn't provide a protocol class for this command.
   */
  def hlen(key: RKey) = pool {
    state.read { m =>
      m get(key) match {
        case Some(Entry(RHash(hash), _)) => IntegerReply(hash.size)
        case Some(_) => Replies.errWrongType
        case None => IntegerReply(0)
      }
    }
  }

  def hmget(key: RKey, fields: Seq[RKey]) = pool {
    if (fields.isEmpty)
      Replies.errWrongNumArgs("hmget")
    else {
      state.read { m =>
        m get(key) match {
          case Some(Entry(RHash(hash), _)) => {
            val values = fields.map { f =>
                hash.get(f).map(bulkReply(_)).getOrElse(EmptyBulkReply())
            }.toList
            MBulkReply(values)
          }
          case Some(_) => Replies.errWrongType
          case None => MBulkReply(List.fill(fields.size)(EmptyBulkReply()))
        }
      }
    }
  }

  def hset(key: RKey, field: RKey, value: Array[Byte]) = pool {
    state.update { m =>
      m get(key) match {
        case Some(Entry(RHash(hash), expiry)) => {
          val alreadyContainsField = hash.contains(field)
          val newHash = hash + (field -> value)
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
          val updated = m + (key -> Entry(RHash(Map(field -> value))))
          updateAndReply(updated, IntegerReply(1))
        }
      }
    }
  }

}
