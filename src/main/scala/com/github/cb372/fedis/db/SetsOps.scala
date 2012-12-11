package com.github.cb372.fedis.db

import com.twitter.finagle.redis.protocol._

/**
 * Author: chris
 * Created: 6/27/12
 */

trait SetsOps extends ReplyFactory {
  this: DbCommon =>

  def sadd(key: RKey, members: Seq[RKey]) = pool {
    state.update { m =>
      m get(key) match {
        case Some(Entry(RSet(set), expiry)) => {
          val addCount = members.count(!set.contains(_))
          if (addCount > 0) {
            val newSet = set ++ members
            val updated = m + (key -> Entry(RSet(newSet), expiry)) // copy expiry
            updateAndReply(updated, IntegerReply(addCount))
          } else
            noUpdate(IntegerReply(0))
        }
        case Some(_) => noUpdate(Replies.errWrongType)
        case None => {
          val updated = m + (key -> Entry(RSet(members.toSet)))
          updateAndReply(updated, IntegerReply(members.size))
        }
      }
    }
  }

  def scard(key: RKey) = pool {
    state.read { m =>
      m get(key) match {
        case Some(Entry(RSet(set), _)) => {
          IntegerReply(set.size)
        }
        case Some(_) => Replies.errWrongType
        case None => IntegerReply(0)
      }
    }
  }

  def sismember(key: RKey, member: RKey) = pool {
    state.read { m =>
      m get(key) match {
        case Some(Entry(RSet(set), _)) => {
          if (set.contains(member))
            IntegerReply(1)
          else
            IntegerReply(0)
        }
        case Some(_) => Replies.errWrongType
        case None => IntegerReply(0)
      }
    }
  }

  def smembers(key: RKey) = pool {
    state.read { m =>
      m get(key) match {
        case Some(Entry(RSet(set), _)) => {
          val members = set.map(x => bulkReply(x.array)).toList
          MBulkReply(members)
        }
        case Some(_) => Replies.errWrongType
        case None => EmptyMBulkReply()
      }
    }
  }

  def srem(key: RKey, members: Seq[RKey]) = pool {
    state.update { m =>
      m get(key) match {
        case Some(Entry(RSet(set), expiry)) => {
          val deleteCount = members.count(set.contains(_))
          if (deleteCount > 0) {
            val newSet = set -- members
            val updated =
              if (newSet.isEmpty)
                // delete the key
                m - key
              else
                m + (key -> Entry(RSet(newSet), expiry)) // copy expiry
            updateAndReply(updated, IntegerReply(deleteCount))
          } else
            noUpdate(IntegerReply(0))
        }
        case Some(_) => noUpdate(Replies.errWrongType)
        case None => noUpdate(IntegerReply(0))
      }
    }
  }

}
