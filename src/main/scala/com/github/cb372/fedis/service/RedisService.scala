package com.github.cb372.fedis
package service

import com.twitter.finagle.Service
import java.util.concurrent.Executors
import com.twitter.util.{Future, FuturePool}
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.ServerError
import db.Db

class RedisService extends Service[SessionAndCommand, Reply] {
  private val pool = FuturePool(Executors.newFixedThreadPool(4))

  private val dbs = Array.fill(Constants.maxDbIndex){new Db(pool)}

  def apply(req: SessionAndCommand): Future[Reply] = {
    // Choose the appropriate DB for the client
    val db = dbs(req.session.db)

    req.cmd match {
      case Get(key: String) => db.get(key)
      case Set(key: String, value: Array[Byte]) => db.set(key, value)
      case Del(keys: List[_]) => db.del(keys)
      case Exists(key: String) => db.exists(key)
      case Append(key: String, suffix: Array[Byte]) => db.append(key, suffix)
      case _ => Future.exception(ServerError("Not implemented"))
    }
  }


}

