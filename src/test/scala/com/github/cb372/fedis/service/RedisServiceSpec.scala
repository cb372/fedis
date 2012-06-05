package com.github.cb372.fedis.service

import org.scalatest._
import org.scalatest.matchers._
import com.github.cb372.fedis.{Session, SessionAndCommand}
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.Service
import com.github.cb372.fedis.db.DummyDbTask
import com.twitter.util.{NullTimer, FuturePool, Future}

class RedisServiceSpec extends FlatSpec with ShouldMatchers {

  trait Fixture {
    val mockFuturePool = FuturePool.immediatePool
    val mockTimer = new NullTimer
    val mockReaper = new DummyDbTask

    def createService = new RedisService(mockFuturePool, mockTimer, mockReaper)
  }

  behavior of "RedisService"

  it should "have a db with index 0" in new Fixture {
    val service = createService
    val cmd = SessionAndCommand(Session(false, 0), Exists("foo"))
    val reply = service.apply(cmd)
    reply.get should equal(IntegerReply(0))
  }

  it should "have a db with index 15" in new Fixture {
    val service = createService
    val cmd = SessionAndCommand(Session(false, 15), Exists("foo"))
    val reply = service.apply(cmd)
    reply.get should equal(IntegerReply(0))
  }

  it should "hold separate state for each DB" in new Fixture {
    val service = createService
    val value = "abc".getBytes

    // SET foo abc in DB 1
    service.apply(
      SessionAndCommand(Session(false, 1), Set("foo", value))
    )

    // GET foo in DBs 1 and 2
    val getInDb1 = service.apply(
      SessionAndCommand(Session(false, 1), Get("foo"))
    )
    val getInDb2 = service.apply(
      SessionAndCommand(Session(false, 2), Get("foo"))
    )

    getInDb1.get should equal(BulkReply(value))
    getInDb2.get should equal(EmptyBulkReply())
  }
}
