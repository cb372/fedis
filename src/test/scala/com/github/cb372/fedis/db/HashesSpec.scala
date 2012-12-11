package com.github.cb372.fedis.db

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers
import com.twitter.finagle.redis.protocol._
import com.twitter.util.FuturePool

import DbMatchers._

/**
 * Author: chris
 * Created: 6/2/12
 */

class HashesSpec extends FlatSpec with ShouldMatchers with DbTestUtils {

  behavior of "HDEL"

  it should "return an error if the value is not a hash" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), "abc".getBytes)

    db.hdel(rkey("foo"), rkeys("bar")).get should equal(Replies.errWrongType)
  }

  it should "return 0 if the key does not exist" in {
    val db = new Db(FuturePool.immediatePool)
    db.hdel(rkey("foo"), rkeys("bar", "baz")).get should equal(IntegerReply(0))
  }

  it should "delete the given fields" in {
    val db = new Db(FuturePool.immediatePool)
    db.hset(rkey("foo"), rkey("field1"), "one".getBytes)
    db.hset(rkey("foo"), rkey("field2"), "two".getBytes)
    db.hset(rkey("foo"), rkey("field3"), "three".getBytes)
    db.hset(rkey("foo"), rkey("field4"), "four".getBytes)

    db.hdel(rkey("foo"), rkeys("field1", "field3"))

    db.hget(rkey("foo"), rkey("field1")).get should equal(EmptyBulkReply())
    db.hget(rkey("foo"), rkey("field2")).get should beBulkReplyWithValue("two")
    db.hget(rkey("foo"), rkey("field3")).get should equal(EmptyBulkReply())
    db.hget(rkey("foo"), rkey("field4")).get should beBulkReplyWithValue("four")
  }

  it should "return the number of fields actually deleted" in {
    val db = new Db(FuturePool.immediatePool)
    db.hset(rkey("foo"), rkey("field2"), "two".getBytes)
    db.hset(rkey("foo"), rkey("field4"), "four".getBytes)

    db.hdel(rkey("foo"), rkeys("field1", "field2", "field3", "field4")).get should equal(IntegerReply(2))
  }

  it should "delete the hash if it becomes empty" in {
    val db = new Db(FuturePool.immediatePool)
    db.hset(rkey("foo"), rkey("field2"), "two".getBytes)
    db.hset(rkey("foo"), rkey("field4"), "four".getBytes)

    db.hdel(rkey("foo"), rkeys("field1", "field2", "field3", "field4"))
    db.taipu(rkey("foo")).get should equal(StatusReply("none"))

  }

  behavior of "HGET"

  it should "return an error if the value is not a hash" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), "abc".getBytes)

    db.hget(rkey("foo"), rkey("bar")).get should equal(Replies.errWrongType)
  }

  it should "return nil if the key does not exist" in {
    val db = new Db(FuturePool.immediatePool)
    db.hget(rkey("foo"), rkey("bar")).get should equal(EmptyBulkReply())
  }

  it should "return nil if the key exists but the field does not exist" in {
    val db = new Db(FuturePool.immediatePool)
    db.hset(rkey("foo"), rkey("bar"), "baz".getBytes)

    db.hget(rkey("foo"), rkey("hoge")).get should equal(EmptyBulkReply())
  }

  it should "return the requested field's value if it exists" in {
    val db = new Db(FuturePool.immediatePool)
    db.hset(rkey("foo"), rkey("bar"), "baz".getBytes)

    db.hget(rkey("foo"), rkey("bar")).get should beBulkReplyWithValue("baz")
  }

  behavior of "HGETALL"

  it should "return an error if the value is not a hash" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), "abc".getBytes)

    db.hgetAll(rkey("foo")).get should equal(Replies.errWrongType)
  }

  it should "return an empty list if the key does not exist" in {
    val db = new Db(FuturePool.immediatePool)
    db.hgetAll(rkey("foo")).get should equal(EmptyMBulkReply())
  }

  it should "return all the keys and values in the hash" in {
    val myMap = Map(
      "field1" -> "one",
      "field2" -> "two",
      "field3" -> "three",
      "field4" -> "four"
    )
    val db = new Db(FuturePool.immediatePool)
    for ((k,v) <- myMap) db.hset(rkey("foo"), rkey(k), v.getBytes)

    val list = decodeMBulkReply(db.hgetAll(rkey("foo")).get.asInstanceOf[MBulkReply])
    val resultMap = list.grouped(2).map(xs => (xs(0), xs(1))).toMap

    resultMap should equal(myMap)
  }

  behavior of "HKEYS"

  it should "return an empty list if the key does not exist" in {
    val db = new Db(FuturePool.immediatePool)
    db.hkeys(rkey("foo")).get should equal(EmptyMBulkReply())
  }

  it should "return an error if the value is not a hash" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), "abc".getBytes)

    db.hkeys(rkey("foo")).get should equal(Replies.errWrongType)
  }

  it should "return all the keys in the hash" in {
    val myMap = Map(
      "field1" -> "one",
      "field2" -> "two",
      "field3" -> "three"
    )
    val db = new Db(FuturePool.immediatePool)
    for ((k,v) <- myMap) db.hset(rkey("foo"), rkey(k), v.getBytes)

    val list = decodeMBulkReply(db.hkeys(rkey("foo")).get.asInstanceOf[MBulkReply])
    list.toSet should equal(collection.Set("field1", "field2", "field3"))
  }

  behavior of "HLEN"

  it should "return an error if the value is not a hash" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), "abc".getBytes)

    db.hlen(rkey("foo")).get should equal(Replies.errWrongType)
  }

  it should "return 0 if the field does not exist" in {
    val db = new Db(FuturePool.immediatePool)
    db.hlen(rkey("foo")).get should equal(IntegerReply(0))
  }

  it should "return the number of fields in the hash" in {
    val db = new Db(FuturePool.immediatePool)
    db.hset(rkey("foo"), rkey("bar"), "a".getBytes)
    db.hset(rkey("foo"), rkey("baz"), "b".getBytes)

    db.hlen(rkey("foo")).get should equal(IntegerReply(2))
  }

  behavior of "HMGET"

  it should "return an error if the value is not a hash" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), "abc".getBytes)

    db.hmget(rkey("foo"), rkeys("bar")).get should equal(Replies.errWrongType)
  }

  it should "return an error if no fields are specified" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), "abc".getBytes)

    db.hmget(rkey("foo"), Seq()).get should equal(ErrorReply("ERR wrong number of arguments for 'hmget' command"))
  }

  it should "return empty byte arrays for all fields if key does not exist" in {
    val db = new Db(FuturePool.immediatePool)
    val replies = db.hmget(rkey("foo"), rkeys("a", "b")).get.asInstanceOf[MBulkReply].messages

    replies should have length (2)
    replies(0) should equal(EmptyBulkReply())
    replies(1) should equal(EmptyBulkReply())
  }

  it should "return the specified fields" in {
    val db = new Db(FuturePool.immediatePool)
    db.hset(rkey("foo"), rkey("a"), "123".getBytes)
    db.hset(rkey("foo"), rkey("c"), "456".getBytes)
    val replies = db.hmget(rkey("foo"), rkeys("a", "b", "c")).get.asInstanceOf[MBulkReply].messages

    replies should have length (3)
    replies(0) should beBulkReplyWithValue("123")
    replies(1) should equal(EmptyBulkReply())
    replies(2) should beBulkReplyWithValue("456")
  }

  behavior of "HSET"

  it should "return an error if the value is not a hash" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), "abc".getBytes)

    db.hset(rkey("foo"), rkey("bar"), "baz".getBytes).get should equal(Replies.errWrongType)
  }

  it should "return 1 if the field was added to hash" in {
    val db = new Db(FuturePool.immediatePool)
    db.hset(rkey("foo"), rkey("a"), "b".getBytes)

    val reply = db.hset(rkey("foo"), rkey("bar"), "baz".getBytes).get
    reply should equal(IntegerReply(1))
  }

  it should "return 0 if an existing hash field was updated" in {
    val db = new Db(FuturePool.immediatePool)
    db.hset(rkey("foo"), rkey("bar"), "old".getBytes)

    val reply = db.hset(rkey("foo"), rkey("bar"), "new".getBytes).get
    reply should equal(IntegerReply(0))
  }

}
