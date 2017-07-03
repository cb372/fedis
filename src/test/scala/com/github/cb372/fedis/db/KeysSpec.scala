package com.github.cb372.fedis.db

import org.scalatest.{FlatSpec, Matchers}
//import org.scalatest.matchers.ShouldMatchers
import com.twitter.finagle.redis.protocol._
import com.twitter.util.{FuturePool, Time}
import com.twitter.finagle.redis.util.CBToString
import DbMatchers._
import scala.concurrent.duration._
import com.github.cb372.fedis.util.ImplicitConversions._


/**
 * Author: chris
 * Created: 6/2/12
 */

class KeysSpec extends FlatSpec with Matchers with DbTestUtils {

  behavior of "DEL"

  it should "delete all the specified keys and no others" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), "abc".getBytes)
    db.set(rkey("bar"), "def".getBytes)
    db.set(rkey("baz"), "ghi".getBytes)
    db.set(rkey("hoge"), "jkl".getBytes)

//    val reply = db.del(rkeys("bar", "hoge", "wibble"))
    val reply = db.del(rkeys("bar", "hoge", "wibble")).toJavaFuture.get()

    println(s"$reply=======================================")

    reply should equal(IntegerReply(2))

    db.get(rkey("foo")).toJavaFuture.get()  should beBulkReplyWithValue("abc")
    db.get(rkey("bar")).toJavaFuture.get() should equal(EmptyBulkReply())
    db.get(rkey("baz")).toJavaFuture.get() should beBulkReplyWithValue("ghi")
    db.get(rkey("hoge")).toJavaFuture.get should equal(EmptyBulkReply())
  }

  behavior of "EXPIRE"

  it should "return 0 if key does not exist" in {
    val db = new Db(FuturePool.immediatePool)
    val reply = db.expire(rkey("foo"), 100L).toJavaFuture.get()
  //  val reply = db.expire(rkey("foo"), 100L).result(DurationInt(1))

    reply should equal(IntegerReply(0))
  }

  it should "set expiry to the given time if key exists" in {
    val key = rkey("foo")
    Time.withTimeAt(Time.fromMilliseconds(5000L)) {
      _ =>
        val db = new Db(FuturePool.immediatePool)
        db.set(key, "abc".getBytes)

        val reply = db.expire(key, 100L).toJavaFuture.get()
        reply should equal(IntegerReply(1))

        val expiry = getExpiry(db.iterator, key)
        expiry should equal(Some(Time.fromMilliseconds(5000L + 100L * 1000L)))
    }
  }

  behavior of "EXPIREAT"

  it should "return 0 if key does not exist" in {
    val db = new Db(FuturePool.immediatePool)
    val reply = db.expireAt(rkey("foo"), Time.fromMilliseconds(25000L)).toJavaFuture.get()
    reply should equal(IntegerReply(0))
  }

  it should "set expiry to the given time if key exists" in {
    val key = rkey("foo")
    Time.withTimeAt(Time.fromMilliseconds(5000L)) {
      _ =>
        val db = new Db(FuturePool.immediatePool)
        db.set(key, "abc".getBytes)
        val reply = db.expireAt(key, Time.fromMilliseconds(25000L)).toJavaFuture.get()
        reply should equal(IntegerReply(1))

        val expiry = getExpiry(db.iterator, key)
        expiry should equal(Some(Time.fromMilliseconds(25000L)))
    }
  }

  behavior of "KEYS"

  it should "return an empty list if there are no keys in the DB" in {
    val db = new Db(FuturePool.immediatePool)
    val reply = db.keys("a").toJavaFuture.get
    reply should equal(EmptyMBulkReply())
  }

  it should "return an empty list if there are no matching keys" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), "abc".getBytes)
    val reply = db.keys("a").toJavaFuture.get
    reply should equal(EmptyMBulkReply())
  }

  it should "return all keys if pattern is *" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), "abc".getBytes)
    db.set(rkey("bar"), "def".getBytes)
    db.set(rkey("baz"), "ghi".getBytes)

    val msgs = decodeMBulkReply(db.keys("*").toJavaFuture.get.asInstanceOf[MBulkReply])
    msgs should contain("foo")
    msgs should contain("bar")
    msgs should contain("baz")
  }

  it should "return only keys matching pattern using *" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), "abc".getBytes)
    db.set(rkey("bar"), "def".getBytes)
    db.set(rkey("baz"), "ghi".getBytes)

    val msgs = decodeMBulkReply(db.keys("b*").toJavaFuture.get.asInstanceOf[MBulkReply])
    msgs should not contain("foo")
    msgs should contain("bar")
    msgs should contain("baz")
  }

  it should "return only keys matching pattern using ?" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), "abc".getBytes)
    db.set(rkey("bar"), "def".getBytes)
    db.set(rkey("baz"), "ghi".getBytes)

    val msgs = decodeMBulkReply(db.keys("b??").toJavaFuture.get.asInstanceOf[MBulkReply])
    msgs should not contain("foo")
    msgs should contain("bar")
    msgs should contain("baz")
  }

  it should "return only keys matching pattern using [ab]" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), "abc".getBytes)
    db.set(rkey("bar"), "def".getBytes)
    db.set(rkey("baz"), "ghi".getBytes)

    val msgs = decodeMBulkReply(db.keys("[ab]ar").toJavaFuture.get.asInstanceOf[MBulkReply])
    msgs should not contain("foo")
    msgs should contain("bar")
    msgs should not contain("baz")
  }

  it should "return only keys matching complex pattern" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), "abc".getBytes)
    db.set(rkey("bar"), "def".getBytes)
    db.set(rkey("baz"), "ghi".getBytes)
    db.set(rkey("barbaz"), "jkl".getBytes)

    val msgs = decodeMBulkReply(db.keys("[abc]?[rz]*").toJavaFuture.get.asInstanceOf[MBulkReply])
    msgs should not contain("foo")
    msgs should contain("bar")
    msgs should contain("baz")
    msgs should contain("barbaz")
  }

  it should "respect backslash escapes in pattern" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), "abc".getBytes)
    db.set(rkey("f?o"), "def".getBytes)
    db.set(rkey("f*o"), "def".getBytes)

    val msgs = decodeMBulkReply(db.keys("f\\?o").toJavaFuture.get.asInstanceOf[MBulkReply])
    msgs should not contain("foo")
    msgs should contain("f?o")
    msgs should not contain("f*o")

    val msgs2 = decodeMBulkReply(db.keys("f\\*o").toJavaFuture.get.asInstanceOf[MBulkReply])
    msgs2 should not contain("foo")
    msgs2 should not contain("f?o")
    msgs2 should contain("f*o")
  }

  behavior of "PERSIST"

  it should "return 0 if the key does not exist" in {
    val db = new Db(FuturePool.immediatePool)
    val reply = db.persist(rkey("foo")).toJavaFuture.get()
    reply should equal(IntegerReply(0))
  }

  it should "return 0 if the key has no timeout" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), "abc".getBytes)
    val reply = db.persist(rkey("foo")).toJavaFuture.get()
    reply should equal(IntegerReply(0))
  }

  it should "remove the timeout from an entry" in {
    val key = rkey("foo")
    val db = new Db(FuturePool.immediatePool)
    db.set(key, "abc".getBytes)
    db.expire(key, 100L)

    val reply = db.persist(key).toJavaFuture.get
    reply should equal(IntegerReply(1))

    val expiry = getExpiry(db.iterator, key)
    expiry should equal(None)
  }

  behavior of "RANDOMKEY"

  it should "return nil if there are no keys" in {
    val db = new Db(FuturePool.immediatePool)
    db.randomKey().toJavaFuture.get should equal(EmptyBulkReply())
  }

  it should "return a random key" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), "a".getBytes)
    db.set(rkey("bar"), "b".getBytes)
    db.set(rkey("baz"), "c".getBytes)


    val key = CBToString(db.randomKey().toJavaFuture.get.asInstanceOf[BulkReply].message)
    //key should (equal("foo") or equal("bar") or equal("baz"))
    key should  (equal("foo") or equal("bar") or equal("baz"))
    /*
    key should   equal("foo")
    key should   equal("bar")
    key should   equal("baz")*/

  }

  behavior of "RENAME"

  it should "return an error if the key does not exist" in {
    val db = new Db(FuturePool.immediatePool)
    val reply = db.rename(rkey("foo"), rkey("bar")).toJavaFuture.get()
    reply should equal(Replies.errNoSuchKey)
  }

  it should "return an error if the old and new keys are the same (key does not exist)" in {
    val db = new Db(FuturePool.immediatePool)
    val reply = db.rename(rkey("foo"), rkey("foo")).toJavaFuture.get()
    reply should equal(Replies.errSourceAndDestEqual)
  }

  it should "return an error if the old and new keys are the same (key exists)" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), "avc".getBytes)
    val reply = db.rename(rkey("foo"), rkey("foo")).toJavaFuture.get()
    reply should equal(Replies.errSourceAndDestEqual)
  }

  it should "rename the given key" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), "abc".getBytes)
    val reply = db.rename(rkey("foo"), rkey("bar")).toJavaFuture.get()
    reply should equal(Replies.ok)

    db.get(rkey("foo")).toJavaFuture.get() should equal(EmptyBulkReply())
    db.get(rkey("bar")).toJavaFuture.get() should beBulkReplyWithValue("abc")
  }

  behavior of "RENAMENX"

  it should "return an error if the key does not exist" in {
    val db = new Db(FuturePool.immediatePool)
    val reply = db.renameNx(rkey("foo"), rkey("bar")).toJavaFuture.get()
    reply should equal(Replies.errNoSuchKey)
  }

  it should "return an error if the old and new keys are the same (key does not exist)" in {
    val db = new Db(FuturePool.immediatePool)
    val reply = db.renameNx(rkey("foo"), rkey("foo")).toJavaFuture.get()
    reply should equal(Replies.errSourceAndDestEqual)
  }

  it should "return an error if the old and new keys are the same (key exists)" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), "avc".getBytes)
    val reply = db.renameNx(rkey("foo"), rkey("foo")).toJavaFuture.get()
    reply should equal(Replies.errSourceAndDestEqual)
  }

  it should "rename the given key if the new key does not exist" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), "abc".getBytes)
    val reply = db.renameNx(rkey("foo"), rkey("bar")).toJavaFuture.get()
    reply should equal(IntegerReply(1))

    db.get(rkey("foo")).toJavaFuture.get() should equal(EmptyBulkReply())
    db.get(rkey("bar")).toJavaFuture.get() should beBulkReplyWithValue("abc")
  }

  it should "do nothing if the new key already exists" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), "abc".getBytes)
    db.set(rkey("bar"), "def".getBytes)
    val reply = db.renameNx(rkey("foo"), rkey("bar")).toJavaFuture.get()
    reply should equal(IntegerReply(0))

    db.get(rkey("foo")).toJavaFuture.get() should beBulkReplyWithValue("abc")
    db.get(rkey("bar")).toJavaFuture.get() should beBulkReplyWithValue("def")
  }

  behavior of "TTL"

  it should "return -1 if the key does not exist" in {
    val db = new Db(FuturePool.immediatePool)
    val reply = db.ttl(rkey("foo")).toJavaFuture.get()
    reply should equal(IntegerReply(-1))
  }

  it should "return -1 if the key has no timeout" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), "abc".getBytes)
    val reply = db.ttl(rkey("foo")).toJavaFuture.get()
    reply should equal(IntegerReply(-1))
  }

  it should "return the timeout in seconds if there is one" in {
    Time.withCurrentTimeFrozen {
      _ =>
        val db = new Db(FuturePool.immediatePool)
        db.set(rkey("foo"), "abc".getBytes)
        db.expire(rkey("foo"), 100L)

        val reply = db.ttl(rkey("foo")).toJavaFuture.get()
        reply should equal(IntegerReply(100))
    }
  }

  behavior of "TYPE"

  it should "return 'none' if key does not exist" in {
    val db = new Db(FuturePool.immediatePool)
    val reply = db.taipu(rkey("foo")).toJavaFuture.get()
    reply should equal(StatusReply("none"))
  }

  it should "return 'string' if value is a string" in {
    val db = new Db(FuturePool.immediatePool)
    db.set(rkey("foo"), "abc".getBytes)
    val reply = db.taipu(rkey("foo")).toJavaFuture.get()
    reply should equal(StatusReply("string"))
  }

  it should "return 'hash' if value is a hash" in {
    val db = new Db(FuturePool.immediatePool)
    db.hset(rkey("foo"), rkey("abc"), "def".getBytes)
    val reply = db.taipu(rkey("foo")).toJavaFuture.get()
    reply should equal(StatusReply("hash"))
  }

  it should "return 'list' if value is a list" in {
    val db = new Db(FuturePool.immediatePool)
    db.lpush(rkey("foo"), Seq("hello".getBytes))
    val reply = db.taipu(rkey("foo")).toJavaFuture.get()
    reply should equal(StatusReply("list"))
  }

  /*
  it should "return 'set' if value is a set" in {
    val db = new Db(FuturePool.immediatePool)
    // sadd("foo", ...
    val reply = db.taipu(rkey("foo")).get
    reply should equal(StatusReply("set"))
  }

  it should "return 'zset' if value is a sorted set" in {
    val db = new Db(FuturePool.immediatePool)
    // zadd("foo", ...
    val reply = db.taipu(rkey("foo")).get
    reply should equal(StatusReply("zset"))
  }
  */

}
