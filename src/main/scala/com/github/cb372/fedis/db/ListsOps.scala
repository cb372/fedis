package com.github.cb372.fedis.db

import com.twitter.finagle.redis.protocol._

/**
 * Author: chris
 * Created: 6/27/12
 */

trait ListsOps extends ReplyFactory {
  this: DbCommon =>

  def llen(key: RKey) = pool {
    state.read { m =>
      m get(key) match {
        case Some(Entry(RList(list), _)) => IntegerReply(list.size)
        case Some(_) => Replies.errWrongType
        case None => IntegerReply(0)
      }
    }
  }

  def lpop(key: RKey) = pool {
    state.update { m =>
      m get(key) match {
        // Note: will never have an RList(Nil) because we delete empty lists
        case Some(Entry(RList(x::xs), expiry)) => {
          val newList = xs
          val reply = bulkReply(x)
          val updated =
            if (newList.isEmpty) {
              // delete the key
              m - key
            } else {
              m + (key -> Entry(RList(newList), expiry)) // copy expiry
            }
          updateAndReply(updated, reply)
        }
        case Some(_) => noUpdate(Replies.errWrongType)
        case None => noUpdate(EmptyBulkReply())
      }
    }
  }

  def lpush(key: RKey, values: Seq[Array[Byte]]) = pool {
    state.update { m =>
      m get(key) match {
        case Some(Entry(RList(list), expiry)) => {
          val newList = values.toList ::: list
          val reply = IntegerReply(newList.size)
          val updated = m + (key -> Entry(RList(newList), expiry)) // copy expiry
          updateAndReply(updated, reply)
        }
        case Some(_) => noUpdate(Replies.errWrongType)
        case None => {
          val updated = m + (key -> Entry(RList(values.toList)))
          updateAndReply(updated, IntegerReply(values.size))
        }
      }
    }
  }

  def rpop(key: RKey) = pool {
    state.update { m =>
      m get(key) match {
        case Some(Entry(RList(list), expiry)) => {
          val newList = list.dropRight(1)
          val reply = bulkReply(list.last)
          val updated =
            if (newList.isEmpty) {
              // delete the key
              m - key
            } else {
              m + (key -> Entry(RList(newList), expiry)) // copy expiry
            }
          updateAndReply(updated, reply)
        }
        case Some(_) => noUpdate(Replies.errWrongType)
        case None => noUpdate(EmptyBulkReply())
      }
    }
  }

  def rpush(key: RKey, values: Seq[Array[Byte]]) = pool {
    state.update { m =>
      m get(key) match {
        case Some(Entry(RList(list), expiry)) => {
          val newList = list ::: values.toList
          val reply = IntegerReply(newList.size)
          val updated = m + (key -> Entry(RList(newList), expiry)) // copy expiry
          updateAndReply(updated, reply)
        }
        case Some(_) => noUpdate(Replies.errWrongType)
        case None => {
          val updated = m + (key -> Entry(RList(values.toList)))
          updateAndReply(updated, IntegerReply(values.size))
        }
      }
    }
  }
}
