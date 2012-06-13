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

  behavior of "HDEL"

  it should "return an error if the value is not a hash" in {
    val db = new Db(FuturePool.immediatePool)
    db.set("foo", "abc".getBytes)

    db.hdel("foo", Seq("bar")).get should equal(Replies.errWrongType)
  }

  it should "return 0 if the key does not exist" in {
    val db = new Db(FuturePool.immediatePool)
    db.hdel("foo", Seq("bar", "baz")).get should equal(IntegerReply(0))
  }

  it should "delete the given fields" in {
    val db = new Db(FuturePool.immediatePool)
    db.hset("foo", "field1".getBytes, "one".getBytes)
    db.hset("foo", "field2".getBytes, "two".getBytes)
    db.hset("foo", "field3".getBytes, "three".getBytes)
    db.hset("foo", "field4".getBytes, "four".getBytes)

    db.hdel("foo", Seq("field1", "field3"))

    db.hget("foo", "field1".getBytes).get should equal(EmptyBulkReply())
    db.hget("foo", "field2".getBytes).get.asInstanceOf[BulkReply].message should equal("two".getBytes)
    db.hget("foo", "field3".getBytes).get should equal(EmptyBulkReply())
    db.hget("foo", "field4".getBytes).get.asInstanceOf[BulkReply].message should equal("four".getBytes)
  }

  it should "return the number of fields actually deleted" in {
    val db = new Db(FuturePool.immediatePool)
    db.hset("foo", "field2".getBytes, "two".getBytes)
    db.hset("foo", "field4".getBytes, "four".getBytes)

    db.hdel("foo", Seq("field1", "field2", "field3", "field4")).get should equal(IntegerReply(2))
  }


  behavior of "HGET"

  it should "return an error if the value is not a hash" in {
    val db = new Db(FuturePool.immediatePool)
    db.set("foo", "abc".getBytes)

    db.hget("foo", "bar".getBytes).get should equal(Replies.errWrongType)
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

  behavior of "HGETALL"

  it should "return an error if the value is not a hash" in {
    val db = new Db(FuturePool.immediatePool)
    db.set("foo", "abc".getBytes)

    db.hgetAll("foo").get should equal(Replies.errWrongType)
  }

  it should "return an empty list if the key does not exist" in {
    val db = new Db(FuturePool.immediatePool)
    db.hgetAll("foo").get should equal(EmptyMBulkReply())
  }

  it should "return all the keys and values in the hash" in {
    val myMap = Map(
      "field1" -> "one",
      "field2" -> "two",
      "field3" -> "three",
      "field4" -> "four"
    )
    val db = new Db(FuturePool.immediatePool)
    for ((k,v) <- myMap) db.hset("foo", k.getBytes, v.getBytes)

    val list = db.hgetAll("foo").get.asInstanceOf[MBulkReply].messages
    val resultMap = list.grouped(2).map(xs => (new String(xs(0)), new String(xs(1)))).toMap

    resultMap should equal(myMap)
  }

  behavior of "HLEN"

  it should "return an error if the value is not a hash" in {
    val db = new Db(FuturePool.immediatePool)
    db.set("foo", "abc".getBytes)

    db.hlen("foo").get should equal(Replies.errWrongType)
  }

  it should "return 0 if the field does not exist" in {
    val db = new Db(FuturePool.immediatePool)
    db.hlen("foo").get should equal(IntegerReply(0))
  }

  it should "return the number of fields in the hash" in {
    val db = new Db(FuturePool.immediatePool)
    db.hset("foo", "bar".getBytes, "a".getBytes)
    db.hset("foo", "baz".getBytes, "b".getBytes)

    db.hlen("foo").get should equal(IntegerReply(2))
  }

  behavior of "HSET"

  it should "return an error if the value is not a hash" in {
    val db = new Db(FuturePool.immediatePool)
    db.set("foo", "abc".getBytes)

    db.hset("foo", "bar".getBytes, "baz".getBytes).get should equal(Replies.errWrongType)
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
