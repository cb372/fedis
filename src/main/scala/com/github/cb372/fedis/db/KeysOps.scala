package com.github.cb372.fedis.db

import com.twitter.finagle.redis.protocol.{BulkReply, EmptyBulkReply, IntegerReply}
import com.twitter.util.Time
import com.twitter.conversions.time._

/**
 * Author: chris
 * Created: 6/27/12
 */

trait KeysOps { this: DbCommon =>

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

}
