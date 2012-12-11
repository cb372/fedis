package com.github.cb372.fedis
package service

import com.twitter.finagle.Service
import com.twitter.finagle.redis.ServerError
import com.twitter.finagle.redis.protocol._
import com.twitter.util.{Timer, Future, FuturePool}
import com.twitter.conversions.time._
import db.{RKey, KeyValueStoreTask, Db}
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.util.CharsetUtil

class RedisService(pool: FuturePool, timer: Timer, reaper: KeyValueStoreTask)
  extends Service[SessionAndCommand, Reply] {

  private val dbs = List.fill(Constants.numDbs){new Db(pool)}

  // run the expired-values reaper once a second
  timer.schedule(1 second){reaper.run(dbs)}

  import com.github.cb372.fedis.util.ImplicitConversions._

  def apply(req: SessionAndCommand): Future[Reply] = {
    // Choose the appropriate DB for the client
    val db = dbs(req.session.db)

    req.cmd match {
        /*
         * Keys
         */
      case Del(keys) => db.del(keys.map(channelBufferToRKey((_))))
      case Exists(key) => db.exists(key)
      case Expire(key, seconds) => db.expire(key, seconds)
      case ExpireAt(key, timestamp) => db.expireAt(key, timestamp)
      case Keys(pattern) => db.keys(pattern)
      case Persist(key) => db.persist(key)
      case Randomkey() => db.randomKey()
      case Rename(key, newKey) => db.rename(key, newKey)
      case RenameNx(key, newKey) => db.renameNx(key, newKey)
      case Ttl(key) => db.ttl(key)
      case Type(key) => db.taipu(key)

        /*
         * Strings
         */
      case Append(key, suffix) => db.append(key, suffix)
      case Decr(key) => db.decr(key)
      case decrby: DecrBy => db.decrBy(decrby.key, decrby.amount)
      case Get(key) => db.get(key)
      case GetBit(key, offset) => db.getBit(key, offset)
      case GetRange(key, start, end) => db.getRange(key, start, end)
      case GetSet(key, value) => db.getSet(key, value)
      case Incr(key) => db.incr(key)
      case incrby: IncrBy => db.incrBy(incrby.key, incrby.amount) // IncrBy is not a case class :(
      case MGet(keys) => db.mget(keys.map(channelBufferToRKey(_)))
      case MSet(kv) => db.mset(kv)
      case MSetNx(kv) => db.msetNx(kv)
      case Set(key, value) => db.set(key, value)
      case SetBit(key, offset, value) => db.setBit(key, offset, value)
      case SetEx(key, seconds, value) => db.setEx(key, seconds, value)
      case SetNx(key, value) => db.setNx(key, value)
      case SetRange(key, offset, value) => db.setRange(key, offset, value)
      case Strlen(key) => db.strlen(key)

        /*
         * Hashes
         */
      case HDel(key, fields) => db.hdel(key, fields.map(channelBufferToRKey(_)))
      case HGet(key, field) => db.hget(key, field)
      case HGetAll(key) => db.hgetAll(key)
      case HKeys(key) => db.hkeys(key)
      case HMGet(key, fields) => db.hmget(key, fields.map(channelBufferToRKey(_)))
      case HSet(key, field, value) => db.hset(key, field, value)

        /*
         * Lists
         */
      case LLen(key) => db.llen(key)
      case LPop(key) => db.lpop(key)
      case LPush(key, values) => db.lpush(key, values.map(channelBufferToByteArray(_)))
      case RPop(key) => db.rpop(key)
      case RPush(key, values) => db.rpush(key, values.map(channelBufferToByteArray(_)))

        /*
         * Sets
         */
      case SAdd(key, values) => db.sadd(key, values.map(channelBufferToRKey(_)))
      case SCard(key) => db.scard(key)
      case SIsMember(key, value) => db.sismember(key, value)
      case SMembers(key) => db.smembers(key)
      case SRem(key, values) => db.srem(key, values.map(channelBufferToRKey(_)))

      case _ => Future.exception(ServerError("Not implemented"))
    }
  }


}

