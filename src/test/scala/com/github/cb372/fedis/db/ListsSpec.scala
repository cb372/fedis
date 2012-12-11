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

class ListsSpec extends FlatSpec with ShouldMatchers with DbTestUtils {

  behavior of "LLEN"

  it should "return an error if the value is not a list" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), "abc".getBytes)

    db.llen(rkey("foo")).get should equal(Replies.errWrongType)
  }

  it should "return 0 if the key does not exist" in {
    val db = new Db(FuturePool.immediatePool)
    db.llen(rkey("foo")).get should equal(IntegerReply(0))
  }

  it should "return the number of values in the list" in {
    val db = new Db(FuturePool.immediatePool)
    db.lpush(rkey("foo"), Seq("a".getBytes, "b".getBytes))

    db.llen(rkey("foo")).get should equal(IntegerReply(2))
  }

  behavior of "LPOP"

  it should "return an error if the value is not a list" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), "abc".getBytes)

    db.lpop(rkey("foo")).get should equal(Replies.errWrongType)
  }

  it should "return an empty reply if the key does not exist" in {
    val db = new Db(FuturePool.immediatePool)

    val reply = db.lpop(rkey("foo")).get
    reply should equal(EmptyBulkReply())
  }

  it should "return the first value in the list" in {
    val db = new Db(FuturePool.immediatePool)
    db.lpush(rkey("foo"), Seq("a".getBytes, "b".getBytes))

    val reply = db.lpop(rkey("foo")).get
    reply should beBulkReplyWithValue("a")
  }

  it should "remove the value that it returns" in {
    val db = new Db(FuturePool.immediatePool)
    db.lpush(rkey("foo"), Seq("a".getBytes, "b".getBytes))

    db.llen(rkey("foo")).get should equal(IntegerReply(2))
    db.lpop(rkey("foo"))
    db.llen(rkey("foo")).get should equal(IntegerReply(1))
    db.lpop(rkey("foo")).get should beBulkReplyWithValue("b")
  }

  it should "delete the list if it becomes empty" in {
    val db = new Db(FuturePool.immediatePool)
    db.lpush(rkey("foo"), Seq("a".getBytes, "b".getBytes))

    db.lpop(rkey("foo"))
    db.lpop(rkey("foo"))
    db.taipu(rkey("foo")).get should equal(StatusReply("none"))
  }

  behavior of "LPUSH"

  it should "return an error if the value is not a list" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), "abc".getBytes)

    db.lpush(rkey("foo"), Seq("bar".getBytes)).get should equal(Replies.errWrongType)
  }

  it should "return the number of values added if it creates a new list" in {
    val db = new Db(FuturePool.immediatePool)

    val reply = db.lpush(rkey("foo"), Seq("a".getBytes, "b".getBytes)).get
    reply should equal(IntegerReply(2))
  }

  it should "return the new length of the list" in {
    val db = new Db(FuturePool.immediatePool)

    db.lpush(rkey("foo"), Seq("a".getBytes, "b".getBytes)).get
    val reply = db.lpush(rkey("foo"), Seq("a".getBytes, "b".getBytes)).get
    reply should equal(IntegerReply(4))
  }

  behavior of "RPOP"

  it should "return an error if the value is not a list" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), "abc".getBytes)

    db.rpop(rkey("foo")).get should equal(Replies.errWrongType)
  }

  it should "return an empty reply if the key does not exist" in {
    val db = new Db(FuturePool.immediatePool)

    val reply = db.rpop(rkey("foo")).get
    reply should equal(EmptyBulkReply())
  }

  it should "return the last value in the list" in {
    val db = new Db(FuturePool.immediatePool)
    db.lpush(rkey("foo"), Seq("a".getBytes, "b".getBytes))

    val reply = db.rpop(rkey("foo")).get
    reply should beBulkReplyWithValue("b")
  }

  it should "remove the value that it returns" in {
    val db = new Db(FuturePool.immediatePool)
    db.lpush(rkey("foo"), Seq("a".getBytes, "b".getBytes))

    db.llen(rkey("foo")).get should equal(IntegerReply(2))
    db.rpop(rkey("foo"))
    db.llen(rkey("foo")).get should equal(IntegerReply(1))
    db.rpop(rkey("foo")).get should beBulkReplyWithValue("a")
  }

  it should "delete the list if it becomes empty" in {
    val db = new Db(FuturePool.immediatePool)
    db.lpush(rkey("foo"), Seq("a".getBytes, "b".getBytes))

    db.rpop(rkey("foo"))
    db.rpop(rkey("foo"))
    db.taipu(rkey("foo")).get should equal(StatusReply("none"))
  }

  behavior of "RPUSH"

  it should "return an error if the value is not a list" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), "abc".getBytes)

    db.rpush(rkey("foo"), Seq("bar".getBytes)).get should equal(Replies.errWrongType)
  }

  it should "return the number of values added if it creates a new list" in {
    val db = new Db(FuturePool.immediatePool)

    val reply = db.rpush(rkey("foo"), Seq("a".getBytes, "b".getBytes)).get
    reply should equal(IntegerReply(2))
  }

  it should "return the new length of the list" in {
    val db = new Db(FuturePool.immediatePool)

    db.rpush(rkey("foo"), Seq("a".getBytes, "b".getBytes)).get
    val reply = db.rpush(rkey("foo"), Seq("a".getBytes, "b".getBytes)).get
    reply should equal(IntegerReply(4))
  }

}
