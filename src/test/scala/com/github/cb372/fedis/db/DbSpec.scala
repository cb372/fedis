package com.github.cb372.fedis.db

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers
import com.twitter.util.FuturePool
import com.twitter.finagle.redis.protocol.{BulkReply, ErrorReply, IntegerReply}

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

}
