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

class SetsSpec extends FlatSpec with ShouldMatchers with DbTestUtils {

  behavior of "SADD"

  it should "return an error if the value is not a set" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), "abc".getBytes)

    db.sadd(rkey("foo"), rkeys("a", "b")).get should equal(Replies.errWrongType)
  }

  it should "return the number of members added" in {
    val db = new Db(FuturePool.immediatePool)

    db.sadd(rkey("foo"), rkeys("b", "c"))
    db.sadd(rkey("foo"), rkeys("a", "b", "c", "d")).get should equal(IntegerReply(2))
  }

  it should "add members that are not already present" in {
    val db = new Db(FuturePool.immediatePool)

    db.sadd(rkey("foo"), rkeys("b", "c"))
    db.scard(rkey("foo")).get should equal(IntegerReply(2))
    db.sadd(rkey("foo"), rkeys("a", "b", "c", "d"))
    db.scard(rkey("foo")).get should equal(IntegerReply(4))
  }

  behavior of "SCARD"

  it should "return an error if the value is not a set" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), "abc".getBytes)

    db.scard(rkey("foo")).get should equal(Replies.errWrongType)
  }

  it should "return 0 if the field does not exist" in {
    val db = new Db(FuturePool.immediatePool)
    db.scard(rkey("foo")).get should equal(IntegerReply(0))
  }

  it should "return the number of fields in the set" in {
    val db = new Db(FuturePool.immediatePool)
    db.sadd(rkey("foo"), rkeys("bar", "baz"))

    db.scard(rkey("foo")).get should equal(IntegerReply(2))
  }

  behavior of "SISMEMBER"

  it should "return an error if the value is not a set" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), "abc".getBytes)

    db.sismember(rkey("foo"), rkey("bar")).get should equal(Replies.errWrongType)
  }

  it should "return 0 if the key does not exist" in {
    val db = new Db(FuturePool.immediatePool)
    db.sismember(rkey("foo"), rkey("bar")).get should equal(IntegerReply(0))
  }

  it should "return 0 if the value is not a member of the set" in {
    val db = new Db(FuturePool.immediatePool)
    db.sadd(rkey("foo"), rkeys("one", "two", "three"))

    db.sismember(rkey("foo"), rkey("four")).get should equal(IntegerReply(0))
  }

  it should "return 1 if the value is a member of the set" in {
    val db = new Db(FuturePool.immediatePool)
    db.sadd(rkey("foo"), rkeys("one", "two", "three"))

    db.sismember(rkey("foo"), rkey("two")).get should equal(IntegerReply(1))
  }

  behavior of "SMEMBERS"

  it should "return an error if the value is not a set" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), "abc".getBytes)

    db.smembers(rkey("foo")).get should equal(Replies.errWrongType)
  }

  it should "return an empty list if the key does not exist" in {
    val db = new Db(FuturePool.immediatePool)
    db.smembers(rkey("foo")).get should equal(EmptyMBulkReply())
  }

  it should "return all the members in the set" in {
    val db = new Db(FuturePool.immediatePool)
    db.sadd(rkey("foo"), rkeys("one", "two", "three"))

    val list = decodeMBulkReply(db.smembers(rkey("foo")).get.asInstanceOf[MBulkReply])
    list.toSet should equal(collection.Set("one", "two", "three"))
  }

  behavior of "SREM"

  it should "return an error if the value is not a set" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), "abc".getBytes)

    db.srem(rkey("foo"), rkeys("bar")).get should equal(Replies.errWrongType)
  }

  it should "return 0 if the key does not exist" in {
    val db = new Db(FuturePool.immediatePool)
    db.srem(rkey("foo"), rkeys("bar", "baz")).get should equal(IntegerReply(0))
  }

  it should "delete the given fields" in {
    val db = new Db(FuturePool.immediatePool)
    db.sadd(rkey("foo"), rkeys("field1", "field2", "field3", "field4"))

    db.srem(rkey("foo"), rkeys("field1", "field3"))

    db.sismember(rkey("foo"), rkey("field1")).get should equal(IntegerReply(0))
    db.sismember(rkey("foo"), rkey("field2")).get should equal(IntegerReply(1))
    db.sismember(rkey("foo"), rkey("field3")).get should equal(IntegerReply(0))
    db.sismember(rkey("foo"), rkey("field4")).get should equal(IntegerReply(1))
  }

  it should "return the number of fields actually deleted" in {
    val db = new Db(FuturePool.immediatePool)
    db.sadd(rkey("foo"), rkeys("field2", "field4"))

    db.srem(rkey("foo"), rkeys("field1", "field2", "field3", "field4")).get should equal(IntegerReply(2))
  }

  it should "delete the set if it becomes empty" in {
    val db = new Db(FuturePool.immediatePool)
    db.sadd(rkey("foo"), rkeys("field2", "field4"))

    db.srem(rkey("foo"), rkeys("field1", "field2", "field3", "field4"))
    db.taipu(rkey("foo")).get should equal(StatusReply("none"))

  }

}
