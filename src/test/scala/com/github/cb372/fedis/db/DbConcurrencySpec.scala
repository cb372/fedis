package com.github.cb372.fedis.db

import org.scalatest.matchers.ShouldMatchers
import org.scalatest.fixture
import java.util.concurrent.{CyclicBarrier, TimeUnit, Executors, CountDownLatch}
import com.twitter.util.{Future, FuturePool}
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.util.CBToString

/**
 * Author: chris
 * Created: 6/20/12
 */

class DbConcurrencySpec extends fixture.FlatSpec with ShouldMatchers with DbTestUtils {

  behavior of "Db"

  it should "not lose any writes" in { futurePool =>
    val db = new Db(futurePool)

    val numThreads = 10
    val numHashes = 1000
    val start = new CyclicBarrier(numThreads)
    val finish = new CountDownLatch(numThreads)

    val threads = for (i <- 0 until numThreads)
      yield new WriterThread(db, i, numHashes, start, finish)

    threads foreach(_.start())

    finish.await(30, TimeUnit.SECONDS) should equal(true)

    for {
      i <- 0 until numHashes
      j <- 0 until numThreads
    } {
      db.hget(rkey(i.toString), rkey(j.toString)).get match {
        case reply: BulkReply => {
          val msg = CBToString(reply.message)
          msg should equal(j.toString)
        }
        case reply: EmptyBulkReply => fail("No value with key %s and field %s".format(i,j))
        case reply => fail("Unexpected reply: " + reply.toString)
      }
    }

  }

  type FixtureParam = FuturePool

  def withFixture(test: OneArgTest) {
    val exec = Executors.newFixedThreadPool(10)
    try {
      test(FuturePool(exec))
    } finally {
      exec.shutdownNow()
      exec.awaitTermination(1, TimeUnit.SECONDS)
    }
  }

  class WriterThread(db: Db, myId: Int, numHashes: Int, start: CyclicBarrier, finish: CountDownLatch) extends Thread {
    override def run() {
      start.await()

      val field = rkey(myId.toString)
      val value = myId.toString.getBytes

      // send lots of writes
      val futures: Seq[Future[Reply]] =
        for (k <- 0 until numHashes)
          yield db.hset(rkey(k.toString), field, value)

      // wait for them all to finish
      Future.join(futures)

      finish.countDown()
    }
  }
}
