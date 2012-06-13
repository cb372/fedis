package com.github.cb372.fedis.db

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers
import com.twitter.finagle.redis.protocol._
import com.twitter.util.{Time, FuturePool}

/**
 * Author: chris
 * Created: 6/2/12
 */

class DbSpec extends FlatSpec with ShouldMatchers {

  behavior of "INCRBY"

  it should "allow incrementing up to exactly INT_MAX" in {
    val db = new Db(FuturePool.immediatePool)
    db.set("foo", (Int.MaxValue - 10).toString.getBytes)
    db.incrBy("foo", 10).get should equal(IntegerReply(Int.MaxValue))
  }

  it should "not allow integer overflow" in {
    val db = new Db(FuturePool.immediatePool)
    db.set("foo", (Int.MaxValue - 10).toString.getBytes)
    db.incrBy("foo", 11).get should equal(ErrorReply("ERR increment or decrement would overflow"))
  }

  behavior of "DECRBY"

  it should "allow decrementing down to exactly INT_MIN" in {
    val db = new Db(FuturePool.immediatePool)
    db.set("foo", (Int.MinValue + 10).toString.getBytes)
    db.decrBy("foo", 10).get should equal(IntegerReply(Int.MinValue))
  }

  it should "not allow integer overflow" in {
    val db = new Db(FuturePool.immediatePool)
    db.set("foo", (Int.MinValue + 10).toString.getBytes)
    db.decrBy("foo", 11).get should equal(ErrorReply("ERR increment or decrement would overflow"))
  }

  behavior of "GETBIT"

  it should "return 0 if key does not exist" in {
    val db = new Db(FuturePool.immediatePool)
    db.getBit("foo", 10).get should equal(IntegerReply(0))
  }
  it should "return 0 if offset is beyond end of key" in {
    val db = new Db(FuturePool.immediatePool)
    db.set("foo", Array(0x01.toByte, 0x01.toByte))
    db.getBit("foo", 16).get should equal(IntegerReply(0))
  }
  it should "return 0 if kth bit is a zero" in {
    val db = new Db(FuturePool.immediatePool)
    db.set("foo", Array(0x01.toByte, 0x01.toByte))
    db.getBit("foo", 0).get should equal(IntegerReply(0))
  }
  it should "return 1 if kth bit is a one" in {
    val db = new Db(FuturePool.immediatePool)
    db.set("foo", Array(0x01.toByte, 0x01.toByte))
    db.getBit("foo", 15).get should equal(IntegerReply(1))
  }

  behavior of "SETBIT"

  it should "reject any value other than 0 or 1" in {
    val db = new Db(FuturePool.immediatePool)
    db.setBit("foo", 10, 2).get should equal(ErrorReply("ERR bit is not an integer or out of range"))
  }

  it should "create a value with the given bit set if no value exists" in {
    val db = new Db(FuturePool.immediatePool)
    // set bit 10 to 1
    db.setBit("foo", 10, 1).get should equal(IntegerReply(0))
    // get the newly-created value
    val value = db.get("foo").get.asInstanceOf[BulkReply].message
    value should equal(Array(0.toByte, 32.toByte)) // 00000000 00100000
  }

  it should "extend a value if the offset is greater than the value's length" in {
    val db = new Db(FuturePool.immediatePool)

    db.set("foo", Array(0.toByte, 1.toByte))
    // set bit 25 to 1
    db.setBit("foo", 25, 1).get should equal(IntegerReply(0))
    // get the newly-extended value
    val fooValue = db.get("foo").get.asInstanceOf[BulkReply].message
    fooValue should equal(Array(0.toByte, 1.toByte, 0.toByte, 64.toByte)) // 00000000 00000001 00000000 01000000

    db.set("bar", Array(0.toByte, 1.toByte))
    // set bit 25 to 0
    db.setBit("bar", 25, 0).get should equal(IntegerReply(0))
    // get the newly-extended value
    val barValue = db.get("bar").get.asInstanceOf[BulkReply].message
    barValue should equal(Array(0.toByte, 1.toByte, 0.toByte, 0.toByte)) // 00000000 00000001 00000000 00000000
  }

  it should "set the given bit to the given value and return the previous value" in {
    val db = new Db(FuturePool.immediatePool)
    // set bit 10 to 1
    db.setBit("foo", 10, 1).get should equal(IntegerReply(0))
    // get bit 10
    db.getBit("foo", 10).get should equal(IntegerReply(1))
    // now set it to 0
    db.setBit("foo", 10, 0).get should equal(IntegerReply(1))
    // and get it again
    db.getBit("foo", 10).get should equal(IntegerReply(0))
  }

  behavior of "SETNX"

  it should "set the key if it does not already exist" in {
    val db = new Db(FuturePool.immediatePool)
    db.setNx("foo", "hello".getBytes).get should equal(IntegerReply(1))
  }

  it should "do nothing if the key already exists" in {
    val db = new Db(FuturePool.immediatePool)
    db.set("foo", "abc".getBytes)
    db.setNx("foo", "hello".getBytes).get should equal(IntegerReply(0))
    db.get("foo").get.asInstanceOf[BulkReply].message should equal("abc".getBytes)
  }

  behavior of "MGET"

  it should "not allow an empty list" in {
    val db = new Db(FuturePool.immediatePool)
    db.mget(List()).get should equal(ErrorReply("ERR wrong number of arguments for 'mget' command"))
  }

  it should "return empty byte arrays for non-existent keys" in {
    val db = new Db(FuturePool.immediatePool)
    val values = db.mget(List("foo", "bar")).get.asInstanceOf[MBulkReply].messages
    values should have length (2)
    values(0) should have length (0)
    values(1) should have length (0)
  }

  it should "return values for keys that exist" in {
    val db = new Db(FuturePool.immediatePool)
    db.set("foo", "abc".getBytes)
    db.set("bar", "def".getBytes)
    val values = db.mget(List("foo", "bar")).get.asInstanceOf[MBulkReply].messages
    values should have length(2)
    values(0) should equal("abc".getBytes)
    values(1) should equal("def".getBytes)
  }

  behavior of "MSET"

  it should "not allow an empty map" in {
    val db = new Db(FuturePool.immediatePool)
    db.mset(Map()).get should equal(ErrorReply("ERR wrong number of arguments for 'mset' command"))
  }

  it should "set all keys to their corresponding values" in {
    val db = new Db(FuturePool.immediatePool)
    val reply = db.mset(Map("a" -> "1".getBytes, "b" -> "2".getBytes)).get
    reply should equal(StatusReply("OK"))
    db.get("a").get.asInstanceOf[BulkReply].message should equal("1".getBytes)
    db.get("b").get.asInstanceOf[BulkReply].message should equal("2".getBytes)
  }

  private def getExpiry(iterator: Iterator[(String, Entry)], key: String): Option[Time] = {
    iterator.find({case (k, e) => k == key }).flatMap({case (k, e) => e.expiresAt})
  }

  behavior of "EXPIRE"

  it should "return 0 if key does not exist" in {
    val db = new Db(FuturePool.immediatePool)
    val reply = db.expire("foo", 100L).get
    reply should equal(IntegerReply(0))
  }

  it should "set expiry to the given time if key exists" in {
    Time.withTimeAt(Time.fromMilliseconds(5000L)) { _ =>
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
    Time.withTimeAt(Time.fromMilliseconds(5000L)) { _ =>
      val db = new Db(FuturePool.immediatePool)
      db.set("foo", "abc".getBytes)
      val reply = db.expireAt("foo", Time.fromMilliseconds(25000L)).get
      reply should equal(IntegerReply(1))

      val expiry = getExpiry(db.iterator, "foo")
      expiry should equal(Some(Time.fromMilliseconds(25000L)))
    }
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
    Time.withCurrentTimeFrozen { _ =>
      val db = new Db(FuturePool.immediatePool)
      db.set("foo", "abc".getBytes)
      db.expire("foo", 100L)

      val reply = db.ttl("foo").get
      reply should equal(IntegerReply(100))
    }
  }

  behavior of "SETEX"

  it should "set both the value and the expiry" in {
    Time.withTimeAt(Time.fromMilliseconds(5000L)) { _ =>
      val db = new Db(FuturePool.immediatePool)

      val reply = db.setEx("foo", 100L, "abc".getBytes).get
      reply should equal(StatusReply("OK"))

      val expiry = getExpiry(db.iterator, "foo")
      expiry should equal(Some(Time.fromMilliseconds(5000L + 100L * 1000L)))
    }
  }

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
