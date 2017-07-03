package com.github.cb372.fedis.db

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers
import com.twitter.finagle.redis.protocol._
import com.twitter.util.{Time, FuturePool}

import DbMatchers._
import com.github.cb372.fedis.util.ImplicitConversions._
/**
 * Author: chris
 * Created: 6/2/12
 */

class StringsSpec extends FlatSpec with ShouldMatchers with DbTestUtils {

  behavior of "APPEND"

  it should "return an error if the value is not a string" in {
    val db = new Db(FuturePool.immediatePool)
    db.hset(rkey("foo"), rkey("field1"), "abc".getBytes)
    db.append(rkey("foo"), "suffix".getBytes).toJavaFuture.get() should equal(Replies.errWrongType)
  }

  it should "create the value if it does not exist" in {
    val db = new Db(FuturePool.immediatePool)
    val value  = "suffix"
    db.append(rkey("foo"), value.getBytes).toJavaFuture.get() should equal(IntegerReply(value.length))
    db.get(rkey("foo")).toJavaFuture.get() should beBulkReplyWithValue(value)
  }

  it should "append the suffix and return the new value" in {
    val db = new Db(FuturePool.immediatePool)
    val prefix = "prefix".getBytes
    val suffix = "suffix".getBytes
    db.set(rkey("foo"), prefix)
    db.append(rkey("foo"), suffix).toJavaFuture.get() should equal(IntegerReply(prefix.length + suffix.length))
    db.get(rkey("foo")).toJavaFuture.get() should beBulkReplyWithValue("prefixsuffix")
  }

  behavior of "DECRBY"

  it should "return an error if the value is not a string" in {
    val db = new Db(FuturePool.immediatePool)
    db.hset(rkey("foo"), rkey("field1"), "abc".getBytes)
    db.decrBy(rkey("foo"), 10).toJavaFuture.get() should equal(Replies.errWrongType)
  }

  it should "return an error if the value is not an integer" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), "not a number!".getBytes)
    db.decrBy(rkey("foo"), 10).toJavaFuture.get() should equal(Replies.errNotAnInt)
  }

  it should "allow decrementing down to exactly INT_MIN" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), (Int.MinValue + 10).toString.getBytes)
    db.decrBy(rkey("foo"), 10).toJavaFuture.get() should equal(IntegerReply(Int.MinValue))
  }

  it should "not allow integer overflow" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), (Long.MinValue + 10).toString.getBytes)
    db.decrBy(rkey("foo"), 11).toJavaFuture.get() should equal(Replies.errIntOverflow)
  }

  behavior of "GET"

  it should "return an error if the value is not a string" in {
    val db = new Db(FuturePool.immediatePool)
    db.hset(rkey("foo"), rkey("field1"), "abc".getBytes)
    db.get(rkey("foo")).toJavaFuture.get() should equal(Replies.errWrongType)
  }

  it should "return nil if the key does not exist" in {
    val db = new Db(FuturePool.immediatePool)
    db.get(rkey("foo")).toJavaFuture.get() should equal(EmptyBulkReply())
  }

  it should "return the value if the key exists" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), "bar".getBytes)
    db.get(rkey("foo")).toJavaFuture.get() should beBulkReplyWithValue("bar")
  }

  behavior of "GETBIT"

  it should "return an error if the value is not a string" in {
    val db = new Db(FuturePool.immediatePool)
    db.hset(rkey("foo"), rkey("field1"), "abc".getBytes)
    db.getBit(rkey("foo"), 10).toJavaFuture.get() should equal(Replies.errWrongType)
  }

  it should "return 0 if key does not exist" in {
    val db = new Db(FuturePool.immediatePool)
    db.getBit(rkey("foo"), 10).toJavaFuture.get() should equal(IntegerReply(0))
  }

  it should "return 0 if offset is beyond end of key" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), Array(0x01.toByte, 0x01.toByte))
    db.getBit(rkey("foo"), 16).toJavaFuture.get() should equal(IntegerReply(0))
  }
  it should "return 0 if kth bit is a zero" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), Array(0x01.toByte, 0x01.toByte))
    db.getBit(rkey("foo"), 0).toJavaFuture.get() should equal(IntegerReply(0))
  }
  it should "return 1 if kth bit is a one" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), Array(0x01.toByte, 0x01.toByte))
    db.getBit(rkey("foo"), 15).toJavaFuture.get() should equal(IntegerReply(1))
  }

  behavior of "GETRANGE"

  it should "return an error if the value is not a string" in {
    val db = new Db(FuturePool.immediatePool)
    db.hset(rkey("foo"), rkey("field1"), "abc".getBytes)
    db.getRange(rkey("foo"), 1, 2).toJavaFuture.get() should equal(Replies.errWrongType)
  }

  /*
   * I think the 3 commented out tests are actually correct.
   * Server should return "" (empty string) rather than nil.
   * But finagle-redis does not allow this.
   */

  it should "return nil if key does not exist" in {
    val db = new Db(FuturePool.immediatePool)
    db.getRange(rkey("foo"), 1, 2).toJavaFuture.get() should equal(EmptyBulkReply())
  }

  it should "return nil if start is beyond the string's length" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), "abc".getBytes)
    db.getRange(rkey("foo"), 4, 10).toJavaFuture.get() should equal(EmptyBulkReply())
  }

  it should "return nil string if end < start" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), "hello world".getBytes)
    db.getRange(rkey("foo"), 4, 1).toJavaFuture.get() should equal(EmptyBulkReply())
  }
//
//  it should "return empty string if key does not exist" in {
//    val db = new Db(FuturePool.immediatePool)
//    db.getRange(rkey("foo"), 1, 2).get.asInstanceOf[BulkReply].message should equal("".getBytes)
//  }
//
//  it should "return empty string if start is beyond the string's length" in {
//    val db = new Db(FuturePool.immediatePool)
//    db.set(rkey("foo"), "abc".getBytes)
//    db.getRange(rkey("foo"), 4, 10).get.asInstanceOf[BulkReply].message should equal("".getBytes)
//  }
//
//  it should "return empty string if end < start" in {
//    val db = new Db(FuturePool.immediatePool)
//    db.set(rkey("foo"), "hello world".getBytes)
//    db.getRange(rkey("foo"), 4, 1).get.asInstanceOf[BulkReply].message should equal("".getBytes)
//  }

  it should "return one char if end == start" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), "hello world".getBytes)
    db.getRange(rkey("foo"), 6, 6).toJavaFuture.get() should beBulkReplyWithValue("w")
  }

  it should "return the correct substring (start and end are both positive)" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), "hello world".getBytes)
    db.getRange(rkey("foo"), 4, 6).toJavaFuture.get() should beBulkReplyWithValue("o w")
  }

  it should "return the correct substring (start and end are both negative)" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), "hello world".getBytes)
    db.getRange(rkey("foo"), -5, -2).toJavaFuture.get() should beBulkReplyWithValue("worl")
  }

  it should "return the correct substring (start is negative, end is positive)" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), "hello world".getBytes)
    db.getRange(rkey("foo"), -5, 7).toJavaFuture.get() should beBulkReplyWithValue("wo")
  }

  it should "truncate positive index to end of string" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), "hello world".getBytes)
    db.getRange(rkey("foo"), 4, 100).toJavaFuture.get() should beBulkReplyWithValue("o world")
  }

  it should "truncate negative index to start of string" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), "hello world".getBytes)
    db.getRange(rkey("foo"), -100, 4).toJavaFuture.get() should beBulkReplyWithValue("hello")
  }

  behavior of "GETSET"

  it should "return an error if the value is not a string" in {
    val db = new Db(FuturePool.immediatePool)
    db.hset(rkey("foo"), rkey("field1"), "abc".getBytes)
    db.getSet(rkey("foo"), "abc".getBytes).toJavaFuture.get() should equal(Replies.errWrongType)
  }

  it should "set the value and return nil if the key does not exist" in {
    val db = new Db(FuturePool.immediatePool)
    db.getSet(rkey("foo"), "abc".getBytes).toJavaFuture.get() should equal(EmptyBulkReply())
    db.get(rkey("foo")).toJavaFuture.get() should beBulkReplyWithValue("abc")
  }

  it should "set the value and return the old value if the key exists" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), "old".getBytes)
    db.getSet(rkey("foo"), "new".getBytes).toJavaFuture.get() should beBulkReplyWithValue("old")
    db.get(rkey("foo")).toJavaFuture.get() should beBulkReplyWithValue("new")
  }

  behavior of "INCRBY"

  it should "return an error if the value is not a string" in {
    val db = new Db(FuturePool.immediatePool)
    db.hset(rkey("foo"), rkey("field1"), "abc".getBytes)
    db.incrBy(rkey("foo"), 10).toJavaFuture.get() should equal(Replies.errWrongType)
  }

  it should "return an error if the value is not an integer" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), "not a number!".getBytes)
    db.incrBy(rkey("foo"), 10).toJavaFuture.get() should equal(Replies.errNotAnInt)
  }

  it should "allow incrementing up to exactly INT_MAX" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), (Int.MaxValue - 10).toString.getBytes)
    db.incrBy(rkey("foo"), 10).toJavaFuture.get() should equal(IntegerReply(Int.MaxValue))
  }

  it should "not allow integer overflow" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), (Long.MaxValue - 10).toString.getBytes)
    db.incrBy(rkey("foo"), 11).toJavaFuture.get() should equal(Replies.errIntOverflow)
  }

  behavior of "MGET"

  it should "not allow an empty list" in {
    val db = new Db(FuturePool.immediatePool)
    db.mget(List()).toJavaFuture.get() should equal(ErrorReply("ERR wrong number of arguments for 'mget' command"))
  }

  it should "return empty byte arrays for non-existent keys" in {
    val db = new Db(FuturePool.immediatePool)
    val replies = db.mget(rkeys("foo", "bar")).toJavaFuture.get().asInstanceOf[MBulkReply].messages
    replies should have length (2)
    replies(0) should equal(EmptyBulkReply())
    replies(1) should equal(EmptyBulkReply())
  }

  it should "return values for keys that exist" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), "abc".getBytes)
    db.set(rkey("bar"), "def".getBytes)
    val replies = db.mget(rkeys("foo", "bar")).toJavaFuture.get().asInstanceOf[MBulkReply].messages
    replies should have length (2)
    replies(0) should beBulkReplyWithValue("abc")
    replies(1) should beBulkReplyWithValue("def")
  }

  it should "return empty byte arrays for keys with non-string values" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), "abc".getBytes)
    db.hset(rkey("bar"), rkey("fieldKey"), "fieldValue".getBytes)
    db.set(rkey("baz"), "def".getBytes)
    val replies = db.mget(rkeys("foo", "bar", "baz")).toJavaFuture.get().asInstanceOf[MBulkReply].messages
    replies should have length (3)
    replies(0) should beBulkReplyWithValue("abc")
    replies(1) should equal(EmptyBulkReply())
    replies(2) should beBulkReplyWithValue("def")
  }

  behavior of "MSET"

  it should "not allow an empty map" in {
    val db = new Db(FuturePool.immediatePool)
    db.mset(Map()).toJavaFuture.get() should equal(ErrorReply("ERR wrong number of arguments for 'mset' command"))
  }

  it should "set all keys to their corresponding values" in {
    val db = new Db(FuturePool.immediatePool)
    val reply = db.mset(Map(rkey("a") -> "1".getBytes, rkey("b") -> "2".getBytes)).toJavaFuture.get()
    reply should equal(StatusReply("OK"))
    db.get(rkey("a")).toJavaFuture.get() should beBulkReplyWithValue("1")
    db.get(rkey("b")).toJavaFuture.get() should beBulkReplyWithValue("2")
  }

  it should "overwrite both string and non-string values" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("a"), "old1".getBytes)
    db.hset(rkey("b"), rkey("fieldKey"), "fieldValue".getBytes)
    val reply = db.mset(Map(rkey("a") -> "1".getBytes, rkey("b") -> "2".getBytes)).toJavaFuture.get()
    reply should equal(StatusReply("OK"))
    db.get(rkey("a")).toJavaFuture.get() should beBulkReplyWithValue("1")
    db.get(rkey("b")).toJavaFuture.get() should beBulkReplyWithValue("2")
  }

  behavior of "MSETNX"

  it should "not allow an empty map" in {
    val db = new Db(FuturePool.immediatePool)
    db.msetNx(Map()).toJavaFuture.get() should equal(ErrorReply("ERR wrong number of arguments for 'msetnx' command"))
  }

  it should "set all keys to their corresponding values, if none already exist" in {
    val db = new Db(FuturePool.immediatePool)
    val reply = db.msetNx(Map(rkey("a") -> "1".getBytes, rkey("b") -> "2".getBytes)).toJavaFuture.get()
    reply should equal(IntegerReply(1))
    db.get(rkey("a")).toJavaFuture.get() should beBulkReplyWithValue("1")
    db.get(rkey("b")).toJavaFuture.get() should beBulkReplyWithValue("2")
  }

  it should "do nothing if any key already exists" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("a"), "old1".getBytes)
    val reply = db.msetNx(Map(rkey("a") -> "1".getBytes, rkey("b") -> "2".getBytes)).toJavaFuture.get()
    reply should equal(IntegerReply(0))
    db.get(rkey("a")).toJavaFuture.get() should beBulkReplyWithValue("old1")
    db.get(rkey("b")).toJavaFuture.get() should equal(EmptyBulkReply())
  }

  behavior of "SET"

  it should "overwrite an existing value of any type" in {
    val db = new Db(FuturePool.immediatePool)
    db.hset(rkey("foo"), rkey("field1"), "abc".getBytes)
    db.set(rkey("foo"), "bar".getBytes).toJavaFuture.get() should equal(StatusReply("OK"))
    db.get(rkey("foo")).toJavaFuture.get() should beBulkReplyWithValue("bar")
  }

  behavior of "SETBIT"

  it should "return an error if the value is not a string" in {
    val db = new Db(FuturePool.immediatePool)
    db.hset(rkey("foo"), rkey("field1"), "abc".getBytes)
    db.setBit(rkey("foo"), 10, 1).toJavaFuture.get() should equal(Replies.errWrongType)
  }

  it should "reject any value other than 0 or 1" in {
    val db = new Db(FuturePool.immediatePool)
    db.setBit(rkey("foo"), 10, 2).toJavaFuture.get() should equal(Replies.errNotABit)
  }

  it should "create a value with the given bit set if no value exists" in {
    val db = new Db(FuturePool.immediatePool)
    // set bit 10 to 1
    db.setBit(rkey("foo"), 10, 1).toJavaFuture.get() should equal(IntegerReply(0))
    // get the newly-created value
    //val value = db.get(rkey("foo")).toJavaFuture.get().asInstanceOf[BulkReply].message.array
    val value:Array[Byte] = db.get(rkey("foo")).toJavaFuture.get().asInstanceOf[BulkReply].message
    value should equal(Array(0.toByte, 32.toByte)) // 00000000 00100000
  }

  it should "extend a value if the offset is greater than the value's length" in {
    val db = new Db(FuturePool.immediatePool)

    db.set(rkey("foo"), Array(0.toByte, 1.toByte))
    // set bit 25 to 1
    db.setBit(rkey("foo"), 25, 1).toJavaFuture.get() should equal(IntegerReply(0))
    // get the newly-extended value
    //val fooValue = db.get(rkey("foo")).toJavaFuture.get().asInstanceOf[BulkReply].message.array
    val fooValue:Array[Byte] = db.get(rkey("foo")).toJavaFuture.get().asInstanceOf[BulkReply].message

    fooValue should equal(Array(0.toByte, 1.toByte, 0.toByte, 64.toByte)) // 00000000 00000001 00000000 01000000

    db.set(rkey("bar"), Array(0.toByte, 1.toByte))
    // set bit 25 to 0
    db.setBit(rkey("bar"), 25, 0).toJavaFuture.get() should equal(IntegerReply(0))
    // get the newly-extended value
    val barValue:Array[Byte]  = db.get(rkey("bar")).toJavaFuture.get().asInstanceOf[BulkReply].message
    barValue should equal(Array(0.toByte, 1.toByte, 0.toByte, 0.toByte)) // 00000000 00000001 00000000 00000000
  }

  it should "set the given bit to the given value and return the previous value" in {
    val db = new Db(FuturePool.immediatePool)
    // set bit 10 to 1
    db.setBit(rkey("foo"), 10, 1).toJavaFuture.get() should equal(IntegerReply(0))
    // get bit 10
    db.getBit(rkey("foo"), 10).toJavaFuture.get() should equal(IntegerReply(1))
    // now set it to 0
    db.setBit(rkey("foo"), 10, 0).toJavaFuture.get() should equal(IntegerReply(1))
    // and get it again
    db.getBit(rkey("foo"), 10).toJavaFuture.get() should equal(IntegerReply(0))
  }

  behavior of "SETEX"

  it should "set both the value and the expiry" in {
    val key = rkey("foo")
    Time.withTimeAt(Time.fromMilliseconds(5000L)) {
      _ =>
        val db = new Db(FuturePool.immediatePool)

        val reply = db.setEx(key, 100L, "abc".getBytes).toJavaFuture.get()
        reply should equal(StatusReply("OK"))

        val expiry = getExpiry(db.iterator, key)
        expiry should equal(Some(Time.fromMilliseconds(5000L + 100L * 1000L)))
    }
  }

  it should "overwrite an existing value of any type" in {
    val db = new Db(FuturePool.immediatePool)
    db.hset(rkey("foo"), rkey("field1"), "abc".getBytes)
    db.setEx(rkey("foo"), 100L, "bar".getBytes).toJavaFuture.get() should equal(StatusReply("OK"))
    db.get(rkey("foo")).toJavaFuture.get() should beBulkReplyWithValue("bar")
  }

  behavior of "SETNX"

  it should "set the key if it does not already exist" in {
    val db = new Db(FuturePool.immediatePool)
    db.setNx(rkey("foo"), "hello".getBytes).toJavaFuture.get() should equal(IntegerReply(1))
  }

  it should "do nothing if the key already exists" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), "abc".getBytes)
    db.setNx(rkey("foo"), "hello".getBytes).toJavaFuture.get() should equal(IntegerReply(0))
    db.get(rkey("foo")).toJavaFuture.get() should beBulkReplyWithValue("abc")
  }

  it should "do nothing if the key already exists with a non-string type" in {
    val db = new Db(FuturePool.immediatePool)
    db.hset(rkey("foo"), rkey("abc"), "def".getBytes)
    db.setNx(rkey("foo"), "hello".getBytes).toJavaFuture.get() should equal(IntegerReply(0))
    db.hget(rkey("foo"), rkey("abc")).toJavaFuture.get() should beBulkReplyWithValue("def")
  }

  behavior of "SETRANGE"

  it should "return an error if the value is not a string" in {
    val db = new Db(FuturePool.immediatePool)
    db.hset(rkey("foo"), rkey("field1"), "abc".getBytes)
    db.setRange(rkey("foo"), 10, "abc".getBytes).toJavaFuture.get() should equal(Replies.errWrongType)
  }

  it should "return an error if the offset is negative" in {
    val db = new Db(FuturePool.immediatePool)
    db.setRange(rkey("foo"), -1, "abc".getBytes).toJavaFuture.get() should equal(Replies.errOffsetOutOfRange)
  }

  it should "create the key if it does not exist" in {
    val db = new Db(FuturePool.immediatePool)
    val substr = "abc".getBytes
    db.setRange(rkey("foo"), 5, substr).toJavaFuture.get() should equal(IntegerReply(5 + substr.length))
    val value:Array[Byte]  = db.get(rkey("foo")).toJavaFuture.get().asInstanceOf[BulkReply].message
    value should equal(Array[Byte](0,0,0,0,0,'a'.toByte,'b'.toByte,'c'.toByte))
  }

  it should "extend the value if necessary" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), "abc".getBytes)
    val substr = "abc".getBytes
    db.setRange(rkey("foo"), 5, substr).toJavaFuture.get() should equal(IntegerReply(5 + substr.length))
    val value:Array[Byte] = db.get(rkey("foo")).toJavaFuture.get().asInstanceOf[BulkReply].message
    value should equal(Array[Byte]('a'.toByte,'b'.toByte,'c'.toByte,0,0,'a'.toByte,'b'.toByte,'c'.toByte))
  }

  it should "overwrite from the given offset" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), "hello world".getBytes)
    db.setRange(rkey("foo"), 6, "abc".getBytes).toJavaFuture.get() should equal(IntegerReply(11))
    db.get(rkey("foo")).toJavaFuture.get() should beBulkReplyWithValue("hello abcld")
  }

  behavior of "STRLEN"

  it should "return an error if the value is not a string" in {
    val db = new Db(FuturePool.immediatePool)
    db.hset(rkey("foo"), rkey("field1"), "abc".getBytes)
    db.strlen(rkey("foo")).toJavaFuture.get() should equal(Replies.errWrongType)
  }

  it should "return 0 if the key does not exist" in {
    val db = new Db(FuturePool.immediatePool)
    db.strlen(rkey("foo")).toJavaFuture.get() should equal(IntegerReply(0))
  }

  it should "return the length of the value if the key exists" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), "bar".getBytes)
    db.strlen(rkey("foo")).toJavaFuture.get() should equal(IntegerReply(3))
  }
}
