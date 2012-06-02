package com.github.cb372.fedis
package service

import com.twitter.finagle.Service
import java.util.concurrent.Executors
import com.twitter.util.{Future, FuturePool}
import com.twitter.finagle.redis.ServerError
import db.Db
import com.twitter.finagle.redis.protocol._

class RedisService extends Service[SessionAndCommand, Reply] {
  private val pool = FuturePool(Executors.newFixedThreadPool(4))

  private val dbs = Array.fill(Constants.numDbs){new Db(pool)}

  def apply(req: SessionAndCommand): Future[Reply] = {
    // Choose the appropriate DB for the client
    val db = dbs(req.session.db)

    req.cmd match {
        /*
         * Keys
         */
      case Del(keys: List[_]) => db.del(keys)
      case Exists(key: String) => db.exists(key)

        /*
         * Strings
         */
      case Append(key: String, suffix: Array[Byte]) => db.append(key, suffix)
      case Get(key: String) => db.get(key)
      case Decr(key: String) => db.decr(key)
      case decrby: DecrBy => db.decrBy(decrby.key, decrby.amount)
      case Incr(key: String) => db.incr(key)
      case incrby: IncrBy => db.incrBy(incrby.key, incrby.amount) // IncrBy is not a case class :(
      case Set(key: String, value: Array[Byte]) => db.set(key, value)
      case Strlen(key: String) => db.strlen(key)

      case _ => Future.exception(ServerError("Not implemented"))
    }
  }


}

