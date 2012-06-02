package com.github.cb372.fedis
package service

import com.twitter.finagle.Service
import com.twitter.util.{Future, FuturePool}
import com.twitter.finagle.redis.ServerError
import db.Db
import com.twitter.finagle.redis.protocol._

class RedisService(pool: FuturePool) extends Service[SessionAndCommand, Reply] {

  private val dbs = Array.fill(Constants.numDbs){new Db(pool)}

  def apply(req: SessionAndCommand): Future[Reply] = {
    // Choose the appropriate DB for the client
    val db = dbs(req.session.db)

    req.cmd match {
        /*
         * Keys
         */
      case Del(keys) => db.del(keys)
      case Exists(key) => db.exists(key)

        /*
         * Strings
         */
      case Append(key, suffix) => db.append(key, suffix)
      case Decr(key) => db.decr(key)
      case decrby: DecrBy => db.decrBy(decrby.key, decrby.amount)
      case Get(key) => db.get(key)
      case GetBit(key, offset) => db.getBit(key, offset)
      case Incr(key) => db.incr(key)
      case incrby: IncrBy => db.incrBy(incrby.key, incrby.amount) // IncrBy is not a case class :(
      case MGet(keys) => db.mget(keys)
      case MSet(kv) => db.mset(kv)
      case Set(key, value) => db.set(key, value)
      case SetBit(key, offset, value) => db.setBit(key, offset, value)
      case SetNx(key, value) => db.setNx(key, value)
      case Strlen(key) => db.strlen(key)

      case _ => Future.exception(ServerError("Not implemented"))
    }
  }


}

