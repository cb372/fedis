package com.github.cb372.fedis.db

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers
import com.twitter.finagle.redis.protocol._
import com.twitter.util.{Time, FuturePool}

/**
 * Author: chris
 * Created: 6/2/12
 */

class KeysSpec extends FlatSpec with ShouldMatchers with DbTestUtils {

  behavior of "DEL"

  it should "delete all the specified keys and no others" in {
    val db = new Db(FuturePool.immediatePool)
    db.set("foo", "abc".getBytes)
    db.set("bar", "def".getBytes)
    db.set("baz", "ghi".getBytes)
    db.set("hoge", "jkl".getBytes)

    val reply = db.del(List("bar", "hoge", "wibble")).get
    reply should equal(IntegerReply(2))

    db.get("foo").get.asInstanceOf[BulkReply].message should equal("abc".getBytes)
    db.get("bar").get should equal(EmptyBulkReply())
    db.get("baz").get.asInstanceOf[BulkReply].message should equal("ghi".getBytes)
    db.get("hoge").get should equal(EmptyBulkReply())
  }

  behavior of "EXPIRE"

  it should "return 0 if key does not exist" in {
    val db = new Db(FuturePool.immediatePool)
    val reply = db.expire("foo", 100L).get
    reply should equal(IntegerReply(0))
  }

  it should "set expiry to the given time if key exists" in {
    Time.withTimeAt(Time.fromMilliseconds(5000L)) {
      _ =>
        val db = new Db(FuturePool.immediatePool)
        db.set("foo", "abc".getBytes)

        val reply = db.expire("foo", 100L).get
        reply should equal(IntegerReply(1))

        val expiry = getExpiry(db.iterator, "foo")
        expiry should equal(Some(Time.fromMilliseconds(5000L + 100L * 1000L)))
    }
  }

  behavior of "EXPIREAT"

  it should "return 0 if key does not exist" in {
    val db = new Db(FuturePool.immediatePool)
    val reply = db.expireAt("foo", Time.fromMilliseconds(25000L)).get
    reply should equal(IntegerReply(0))
  }

  it should "set expiry to the given time if key exists" in {
    Time.withTimeAt(Time.fromMilliseconds(5000L)) {
      _ =>
        val db = new Db(FuturePool.immediatePool)
        db.set("foo", "abc".getBytes)
        val reply = db.expireAt("foo", Time.fromMilliseconds(25000L)).get
        reply should equal(IntegerReply(1))

        val expiry = getExpiry(db.iterator, "foo")
        expiry should equal(Some(Time.fromMilliseconds(25000L)))
    }
  }

  behavior of "KEYS"

  it should "return an empty list if there are no keys in the DB" in {
    val db = new Db(FuturePool.immediatePool)
    val reply = db.keys("a").get
    reply should equal(EmptyMBulkReply())
  }

  it should "return an empty list if there are no matching keys" in {
    val db = new Db(FuturePool.immediatePool)
    db.set("foo", "abc".getBytes)
    val reply = db.keys("a").get
    reply should equal(EmptyMBulkReply())
  }

  it should "return all keys if pattern is *" in {
    val db = new Db(FuturePool.immediatePool)
    db.set("foo", "abc".getBytes)
    db.set("bar", "def".getBytes)
    db.set("baz", "ghi".getBytes)

    val msgs = decodeMBulkReply(db.keys("*").get.asInstanceOf[MBulkReply])
    msgs should contain("foo")
    msgs should contain("bar")
    msgs should contain("baz")
  }

  it should "return only keys matching pattern using *" in {
    val db = new Db(FuturePool.immediatePool)
    db.set("foo", "abc".getBytes)
    db.set("bar", "def".getBytes)
    db.set("baz", "ghi".getBytes)

    val msgs = decodeMBulkReply(db.keys("b*").get.asInstanceOf[MBulkReply])
    msgs should not contain("foo")
    msgs should contain("bar")
    msgs should contain("baz")
  }

  it should "return only keys matching pattern using ?" in {
    val db = new Db(FuturePool.immediatePool)
    db.set("foo", "abc".getBytes)
    db.set("bar", "def".getBytes)
    db.set("baz", "ghi".getBytes)

    val msgs = decodeMBulkReply(db.keys("b??").get.asInstanceOf[MBulkReply])
    msgs should not contain("foo")
    msgs should contain("bar")
    msgs should contain("baz")
  }

  it should "return only keys matching pattern using [ab]" in {
    val db = new Db(FuturePool.immediatePool)
    db.set("foo", "abc".getBytes)
    db.set("bar", "def".getBytes)
    db.set("baz", "ghi".getBytes)

    val msgs = decodeMBulkReply(db.keys("[ab]ar").get.asInstanceOf[MBulkReply])
    msgs should not contain("foo")
    msgs should contain("bar")
    msgs should not contain("baz")
  }

  it should "return only keys matching complex pattern" in {
    val db = new Db(FuturePool.immediatePool)
    db.set("foo", "abc".getBytes)
    db.set("bar", "def".getBytes)
    db.set("baz", "ghi".getBytes)
    db.set("barbaz", "jkl".getBytes)

    val msgs = decodeMBulkReply(db.keys("[abc]?[rz]*").get.asInstanceOf[MBulkReply])
    msgs should not contain("foo")
    msgs should contain("bar")
    msgs should contain("baz")
    msgs should contain("barbaz")
  }

  it should "respect backslash escapes in pattern" in {
    val db = new Db(FuturePool.immediatePool)
    db.set("foo", "abc".getBytes)
    db.set("f?o", "def".getBytes)
    db.set("f*o", "def".getBytes)

    val msgs = decodeMBulkReply(db.keys("f\\?o").get.asInstanceOf[MBulkReply])
    msgs should not contain("foo")
    msgs should contain("f?o")
    msgs should not contain("f*o")

    val msgs2 = decodeMBulkReply(db.keys("f\\*o").get.asInstanceOf[MBulkReply])
    msgs2 should not contain("foo")
    msgs2 should not contain("f?o")
    msgs2 should contain("f*o")
  }

  behavior of "PERSIST"

  it should "return 0 if the key does not exist" in {
    val db = new Db(FuturePool.immediatePool)
    val reply = db.persist("foo").get
    reply should equal(IntegerReply(0))
  }

  it should "return 0 if the key has no timeout" in {
    val db = new Db(FuturePool.immediatePool)
    db.set("foo", "abc".getBytes)
    val reply = db.persist("foo").get
    reply should equal(IntegerReply(0))
  }

  it should "remove the timeout from an entry" in {
    val db = new Db(FuturePool.immediatePool)
    db.set("foo", "abc".getBytes)
    db.expire("foo", 100L)

    val reply = db.persist("foo").get
    reply should equal(IntegerReply(1))

    val expiry = getExpiry(db.iterator, "foo")
    expiry should equal(None)
  }

  behavior of "RANDOMKEY"

  it should "return nil if there are no keys" in {
    val db = new Db(FuturePool.immediatePool)
    db.randomKey().get should equal(EmptyBulkReply())
  }

  it should "return a random key" in {
    val db = new Db(FuturePool.immediatePool)
    db.set("foo", "a".getBytes)
    db.set("bar", "b".getBytes)
    db.set("baz", "c".getBytes)

    val key = new String(db.randomKey().get.asInstanceOf[BulkReply].message)
    key should (equal("foo") or equal("bar") or equal("baz"))
  }

  behavior of "TTL"

  it should "return -1 if the key does not exist" in {
    val db = new Db(FuturePool.immediatePool)
    val reply = db.ttl("foo").get
    reply should equal(IntegerReply(-1))
  }

  it should "return -1 if the key has no timeout" in {
    val db = new Db(FuturePool.immediatePool)
    db.set("foo", "abc".getBytes)
    val reply = db.ttl("foo").get
    reply should equal(IntegerReply(-1))
  }

  it should "return the timeout in seconds if there is one" in {
    Time.withCurrentTimeFrozen {
      _ =>
        val db = new Db(FuturePool.immediatePool)
        db.set("foo", "abc".getBytes)
        db.expire("foo", 100L)

        val reply = db.ttl("foo").get
        reply should equal(IntegerReply(100))
    }
  }

  behavior of "TYPE"

  it should "return 'none' if key does not exist" in {
    val db = new Db(FuturePool.immediatePool)
    val reply = db.taipu("foo").get
    reply should equal(StatusReply("none"))
  }

  it should "return 'string' if value is a string" in {
    val db = new Db(FuturePool.immediatePool)
    db.set("foo", "abc".getBytes)
    val reply = db.taipu("foo").get
    reply should equal(StatusReply("string"))
  }

  it should "return 'hash' if value is a hash" in {
    val db = new Db(FuturePool.immediatePool)
    db.hset("foo", "abc".getBytes, "def".getBytes)
    val reply = db.taipu("foo").get
    reply should equal(StatusReply("hash"))
  }

  /*
   * Not implemented
   *
  it should "return 'list' if value is a list" in {
    val db = new Db(FuturePool.immediatePool)
    // lpush("foo", ...
    val reply = db.taipu("foo").get
    reply should equal(StatusReply("list"))
  }

  it should "return 'set' if value is a set" in {
    val db = new Db(FuturePool.immediatePool)
    // sadd("foo", ...
    val reply = db.taipu("foo").get
    reply should equal(StatusReply("set"))
  }

  it should "return 'zset' if value is a sorted set" in {
    val db = new Db(FuturePool.immediatePool)
    // zadd("foo", ...
    val reply = db.taipu("foo").get
    reply should equal(StatusReply("zset"))
  }
  */
}
