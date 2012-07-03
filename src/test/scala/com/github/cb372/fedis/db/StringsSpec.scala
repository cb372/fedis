package com.github.cb372.fedis.db

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers
import com.twitter.finagle.redis.protocol._
import com.twitter.util.{Time, FuturePool}

/**
 * Author: chris
 * Created: 6/2/12
 */

class StringsSpec extends FlatSpec with ShouldMatchers with DbTestUtils {

  behavior of "APPEND"

  it should "return an error if the value is not a string" in {
    val db = new Db(FuturePool.immediatePool)
    db.hset("foo", "field1".getBytes, "abc".getBytes)
    db.append("foo", "suffix".getBytes).get should equal(Replies.errWrongType)
  }

  it should "create the value if it does not exist" in {
    val db = new Db(FuturePool.immediatePool)
    val value = "suffix".getBytes
    db.append("foo", value).get should equal(IntegerReply(value.length))
    db.get("foo").get.asInstanceOf[BulkReply].message should equal(value)
  }

  it should "append the suffix and return the new value" in {
    val db = new Db(FuturePool.immediatePool)
    val prefix = "prefix".getBytes
    val suffix = "suffix".getBytes
    db.set("foo", prefix)
    db.append("foo", suffix).get should equal(IntegerReply(prefix.length + suffix.length))
    db.get("foo").get.asInstanceOf[BulkReply].message should equal("prefixsuffix".getBytes)
  }

  behavior of "DECRBY"

  it should "return an error if the value is not a string" in {
    val db = new Db(FuturePool.immediatePool)
    db.hset("foo", "field1".getBytes, "abc".getBytes)
    db.decrBy("foo", 10).get should equal(Replies.errWrongType)
  }

  it should "return an error if the value is not an integer" in {
    val db = new Db(FuturePool.immediatePool)
    db.set("foo", "not a number!".getBytes)
    db.decrBy("foo", 10).get should equal(Replies.errNotAnInt)
  }

  it should "allow decrementing down to exactly INT_MIN" in {
    val db = new Db(FuturePool.immediatePool)
    db.set("foo", (Int.MinValue + 10).toString.getBytes)
    db.decrBy("foo", 10).get should equal(IntegerReply(Int.MinValue))
  }

  it should "not allow integer overflow" in {
    val db = new Db(FuturePool.immediatePool)
    db.set("foo", (Int.MinValue + 10).toString.getBytes)
    db.decrBy("foo", 11).get should equal(Replies.errIntOverflow)
  }

  behavior of "GET"

  it should "return an error if the value is not a string" in {
    val db = new Db(FuturePool.immediatePool)
    db.hset("foo", "field1".getBytes, "abc".getBytes)
    db.get("foo").get should equal(Replies.errWrongType)
  }

  it should "return nil if the key does not exist" in {
    val db = new Db(FuturePool.immediatePool)
    db.get("foo").get should equal(EmptyBulkReply())
  }

  it should "return the value if the key exists" in {
    val db = new Db(FuturePool.immediatePool)
    db.set("foo", "bar".getBytes)
    db.get("foo").get.asInstanceOf[BulkReply].message should equal("bar".getBytes)
  }

  behavior of "GETBIT"

  it should "return an error if the value is not a string" in {
    val db = new Db(FuturePool.immediatePool)
    db.hset("foo", "field1".getBytes, "abc".getBytes)
    db.getBit("foo", 10).get should equal(Replies.errWrongType)
  }

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

  behavior of "GETRANGE"

  it should "return an error if the value is not a string" in {
    val db = new Db(FuturePool.immediatePool)
    db.hset("foo", "field1".getBytes, "abc".getBytes)
    db.getRange("foo", 1, 2).get should equal(Replies.errWrongType)
  }

  /*
   * I think the 3 commented out tests are actually correct.
   * Server should return "" (empty string) rather than nil.
   * But finagle-redis does not allow this.
   */

  it should "return nil if key does not exist" in {
    val db = new Db(FuturePool.immediatePool)
    db.getRange("foo", 1, 2).get should equal(EmptyBulkReply())
  }

  it should "return nil if start is beyond the string's length" in {
    val db = new Db(FuturePool.immediatePool)
    db.set("foo", "abc".getBytes)
    db.getRange("foo", 4, 10).get should equal(EmptyBulkReply())
  }

  it should "return nil string if end < start" in {
    val db = new Db(FuturePool.immediatePool)
    db.set("foo", "hello world".getBytes)
    db.getRange("foo", 4, 1).get should equal(EmptyBulkReply())
  }
//
//  it should "return empty string if key does not exist" in {
//    val db = new Db(FuturePool.immediatePool)
//    db.getRange("foo", 1, 2).get.asInstanceOf[BulkReply].message should equal("".getBytes)
//  }
//
//  it should "return empty string if start is beyond the string's length" in {
//    val db = new Db(FuturePool.immediatePool)
//    db.set("foo", "abc".getBytes)
//    db.getRange("foo", 4, 10).get.asInstanceOf[BulkReply].message should equal("".getBytes)
//  }
//
//  it should "return empty string if end < start" in {
//    val db = new Db(FuturePool.immediatePool)
//    db.set("foo", "hello world".getBytes)
//    db.getRange("foo", 4, 1).get.asInstanceOf[BulkReply].message should equal("".getBytes)
//  }

  it should "return one char if end == start" in {
    val db = new Db(FuturePool.immediatePool)
    db.set("foo", "hello world".getBytes)
    db.getRange("foo", 6, 6).get.asInstanceOf[BulkReply].message should equal("w".getBytes)
  }

  it should "return the correct substring (start and end are both positive)" in {
    val db = new Db(FuturePool.immediatePool)
    db.set("foo", "hello world".getBytes)
    db.getRange("foo", 4, 6).get.asInstanceOf[BulkReply].message should equal("o w".getBytes)
  }

  it should "return the correct substring (start and end are both negative)" in {
    val db = new Db(FuturePool.immediatePool)
    db.set("foo", "hello world".getBytes)
    db.getRange("foo", -5, -2).get.asInstanceOf[BulkReply].message should equal("worl".getBytes)
  }

  it should "return the correct substring (start is negative, end is positive)" in {
    val db = new Db(FuturePool.immediatePool)
    db.set("foo", "hello world".getBytes)
    db.getRange("foo", -5, 7).get.asInstanceOf[BulkReply].message should equal("wo".getBytes)
  }

  it should "truncate positive index to end of string" in {
    val db = new Db(FuturePool.immediatePool)
    db.set("foo", "hello world".getBytes)
    db.getRange("foo", 4, 100).get.asInstanceOf[BulkReply].message should equal("o world".getBytes)
  }

  it should "truncate negative index to start of string" in {
    val db = new Db(FuturePool.immediatePool)
    db.set("foo", "hello world".getBytes)
    db.getRange("foo", -100, 4).get.asInstanceOf[BulkReply].message should equal("hello".getBytes)
  }

  behavior of "GETSET"

  it should "return an error if the value is not a string" in {
    val db = new Db(FuturePool.immediatePool)
    db.hset("foo", "field1".getBytes, "abc".getBytes)
    db.getSet("foo", "abc".getBytes).get should equal(Replies.errWrongType)
  }

  it should "set the value and return nil if the key does not exist" in {
    val db = new Db(FuturePool.immediatePool)
    db.getSet("foo", "abc".getBytes).get should equal(EmptyBulkReply())
    db.get("foo").get.asInstanceOf[BulkReply].message should equal("abc".getBytes)
  }

  it should "set the value and return the old value if the key exists" in {
    val db = new Db(FuturePool.immediatePool)
    db.set("foo", "old".getBytes)
    db.getSet("foo", "new".getBytes).get.asInstanceOf[BulkReply].message should equal("old".getBytes)
    db.get("foo").get.asInstanceOf[BulkReply].message should equal("new".getBytes)
  }

  behavior of "INCRBY"

  it should "return an error if the value is not a string" in {
    val db = new Db(FuturePool.immediatePool)
    db.hset("foo", "field1".getBytes, "abc".getBytes)
    db.incrBy("foo", 10).get should equal(Replies.errWrongType)
  }

  it should "return an error if the value is not an integer" in {
    val db = new Db(FuturePool.immediatePool)
    db.set("foo", "not a number!".getBytes)
    db.incrBy("foo", 10).get should equal(Replies.errNotAnInt)
  }

  it should "allow incrementing up to exactly INT_MAX" in {
    val db = new Db(FuturePool.immediatePool)
    db.set("foo", (Int.MaxValue - 10).toString.getBytes)
    db.incrBy("foo", 10).get should equal(IntegerReply(Int.MaxValue))
  }

  it should "not allow integer overflow" in {
    val db = new Db(FuturePool.immediatePool)
    db.set("foo", (Int.MaxValue - 10).toString.getBytes)
    db.incrBy("foo", 11).get should equal(Replies.errIntOverflow)
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
    values should have length (2)
    values(0) should equal("abc".getBytes)
    values(1) should equal("def".getBytes)
  }

  it should "return empty byte arrays for keys with non-string values" in {
    val db = new Db(FuturePool.immediatePool)
    db.set("foo", "abc".getBytes)
    db.hset("bar", "fieldKey".getBytes, "fieldValue".getBytes)
    db.set("baz", "def".getBytes)
    val values = db.mget(List("foo", "bar", "baz")).get.asInstanceOf[MBulkReply].messages
    values should have length (3)
    values(0) should equal("abc".getBytes)
    values(1) should have length (0)
    values(2) should equal("def".getBytes)
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

  it should "overwrite both string and non-string values" in {
    val db = new Db(FuturePool.immediatePool)
    db.set("a", "old1".getBytes)
    db.hset("b", "fieldKey".getBytes, "fieldValue".getBytes)
    val reply = db.mset(Map("a" -> "1".getBytes, "b" -> "2".getBytes)).get
    reply should equal(StatusReply("OK"))
    db.get("a").get.asInstanceOf[BulkReply].message should equal("1".getBytes)
    db.get("b").get.asInstanceOf[BulkReply].message should equal("2".getBytes)
  }

  behavior of "MSETNX"

  it should "not allow an empty map" in {
    val db = new Db(FuturePool.immediatePool)
    db.msetNx(Map()).get should equal(ErrorReply("ERR wrong number of arguments for 'msetnx' command"))
  }

  it should "set all keys to their corresponding values, if none already exist" in {
    val db = new Db(FuturePool.immediatePool)
    val reply = db.msetNx(Map("a" -> "1".getBytes, "b" -> "2".getBytes)).get
    reply should equal(IntegerReply(1))
    db.get("a").get.asInstanceOf[BulkReply].message should equal("1".getBytes)
    db.get("b").get.asInstanceOf[BulkReply].message should equal("2".getBytes)
  }

  it should "do nothing if any key already exists" in {
    val db = new Db(FuturePool.immediatePool)
    db.set("a", "old1".getBytes)
    val reply = db.msetNx(Map("a" -> "1".getBytes, "b" -> "2".getBytes)).get
    reply should equal(IntegerReply(0))
    db.get("a").get.asInstanceOf[BulkReply].message should equal("old1".getBytes)
    db.get("b").get should equal(EmptyBulkReply())
  }

  behavior of "SET"

  it should "overwrite an existing value of any type" in {
    val db = new Db(FuturePool.immediatePool)
    db.hset("foo", "field1".getBytes, "abc".getBytes)
    db.set("foo", "bar".getBytes).get should equal(StatusReply("OK"))
    db.get("foo").get.asInstanceOf[BulkReply].message should equal("bar".getBytes)
  }

  behavior of "SETBIT"

  it should "return an error if the value is not a string" in {
    val db = new Db(FuturePool.immediatePool)
    db.hset("foo", "field1".getBytes, "abc".getBytes)
    db.setBit("foo", 10, 1).get should equal(Replies.errWrongType)
  }

  it should "reject any value other than 0 or 1" in {
    val db = new Db(FuturePool.immediatePool)
    db.setBit("foo", 10, 2).get should equal(Replies.errNotABit)
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

  behavior of "SETEX"

  it should "set both the value and the expiry" in {
    Time.withTimeAt(Time.fromMilliseconds(5000L)) {
      _ =>
        val db = new Db(FuturePool.immediatePool)

        val reply = db.setEx("foo", 100L, "abc".getBytes).get
        reply should equal(StatusReply("OK"))

        val expiry = getExpiry(db.iterator, "foo")
        expiry should equal(Some(Time.fromMilliseconds(5000L + 100L * 1000L)))
    }
  }

  it should "overwrite an existing value of any type" in {
    val db = new Db(FuturePool.immediatePool)
    db.hset("foo", "field1".getBytes, "abc".getBytes)
    db.setEx("foo", 100L, "bar".getBytes).get should equal(StatusReply("OK"))
    db.get("foo").get.asInstanceOf[BulkReply].message should equal("bar".getBytes)
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

  it should "do nothing if the key already exists with a non-string type" in {
    val db = new Db(FuturePool.immediatePool)
    db.hset("foo", "abc".getBytes, "def".getBytes)
    db.setNx("foo", "hello".getBytes).get should equal(IntegerReply(0))
    db.hget("foo", "abc".getBytes).get.asInstanceOf[BulkReply].message should equal("def".getBytes)
  }

  behavior of "SETRANGE"

  it should "return an error if the value is not a string" in {
    val db = new Db(FuturePool.immediatePool)
    db.hset("foo", "field1".getBytes, "abc".getBytes)
    db.setRange("foo", 10, "abc".getBytes).get should equal(Replies.errWrongType)
  }

  it should "return an error if the offset is negative" in {
    val db = new Db(FuturePool.immediatePool)
    db.setRange("foo", -1, "abc".getBytes).get should equal(Replies.errOffsetOutOfRange)
  }

  it should "create the key if it does not exist" in {
    val db = new Db(FuturePool.immediatePool)
    val substr = "abc".getBytes
    db.setRange("foo", 5, substr).get should equal(IntegerReply(5 + substr.length))
    val value = db.get("foo").get.asInstanceOf[BulkReply].message
    value should equal(Array[Byte](0,0,0,0,0,'a'.toByte,'b'.toByte,'c'.toByte))
  }

  it should "extend the value if necessary" in {
    val db = new Db(FuturePool.immediatePool)
    db.set("foo", "abc".getBytes)
    val substr = "abc".getBytes
    db.setRange("foo", 5, substr).get should equal(IntegerReply(5 + substr.length))
    val value = db.get("foo").get.asInstanceOf[BulkReply].message
    value should equal(Array[Byte]('a'.toByte,'b'.toByte,'c'.toByte,0,0,'a'.toByte,'b'.toByte,'c'.toByte))
  }

  it should "overwrite from the given offset" in {
    val db = new Db(FuturePool.immediatePool)
    db.set("foo", "hello world".getBytes)
    db.setRange("foo", 6, "abc".getBytes).get should equal(IntegerReply(11))
    val value = new String(db.get("foo").get.asInstanceOf[BulkReply].message)
    value should equal("hello abcld")
  }

  behavior of "STRLEN"

  it should "return an error if the value is not a string" in {
    val db = new Db(FuturePool.immediatePool)
    db.hset("foo", "field1".getBytes, "abc".getBytes)
    db.strlen("foo").get should equal(Replies.errWrongType)
  }

  it should "return 0 if the key does not exist" in {
    val db = new Db(FuturePool.immediatePool)
    db.strlen("foo").get should equal(IntegerReply(0))
  }

  it should "return the length of the value if the key exists" in {
    val db = new Db(FuturePool.immediatePool)
    db.set("foo", "bar".getBytes)
    db.strlen("foo").get should equal(IntegerReply(3))
  }
}
