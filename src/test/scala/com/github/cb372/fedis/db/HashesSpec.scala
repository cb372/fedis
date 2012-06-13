package com.github.cb372.fedis.db

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers
import com.twitter.finagle.redis.protocol._
import com.twitter.util.{Time, FuturePool}

/**
 * Author: chris
 * Created: 6/2/12
 */

class HashesSpec extends FlatSpec with ShouldMatchers {

  behavior of "HGET"

  it should "return an error if the value is not a hash" in {
    val db = new Db(FuturePool.immediatePool)
    db.set("foo", "abc".getBytes)

    val reply = db.hget("foo", "bar".getBytes).get
    reply should equal(ErrorReply("ERR Operation against a key holding the wrong kind of value"))
  }

  it should "return nil if the key does not exist" in {
    val db = new Db(FuturePool.immediatePool)
    db.hget("foo", "bar".getBytes).get should equal(EmptyBulkReply())
  }

  it should "return nil if the key exists but the field does not exist" in {
    val db = new Db(FuturePool.immediatePool)
    db.hset("foo", "bar".getBytes, "baz".getBytes)

    db.hget("foo", "hoge".getBytes).get should equal(EmptyBulkReply())
  }

  it should "return the requested field's value if it exists" in {
    val db = new Db(FuturePool.immediatePool)
    db.hset("foo", "bar".getBytes, "baz".getBytes)

    db.hget("foo", "bar".getBytes).get.asInstanceOf[BulkReply].message should equal("baz".getBytes)
  }

  behavior of "HSET"

  it should "return an error if the value is not a hash" in {
    val db = new Db(FuturePool.immediatePool)
    db.set("foo", "abc".getBytes)

    val reply = db.hset("foo", "bar".getBytes, "baz".getBytes).get
    reply should equal(ErrorReply("ERR Operation against a key holding the wrong kind of value"))
  }

  it should "return 1 if the field was added to hash" in {
    val db = new Db(FuturePool.immediatePool)
    db.hset("foo", "a".getBytes, "b".getBytes)

    val reply = db.hset("foo", "bar".getBytes, "baz".getBytes).get
    reply should equal(IntegerReply(1))
  }

  it should "return 0 if an existing hash field was updated" in {
    val db = new Db(FuturePool.immediatePool)
    db.hset("foo", "bar".getBytes, "old".getBytes)

    val reply = db.hset("foo", "bar".getBytes, "new".getBytes).get
    reply should equal(IntegerReply(0))
  }

}
