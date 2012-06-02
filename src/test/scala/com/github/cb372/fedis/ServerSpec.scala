package com.github.cb372.fedis

import org.scalatest._
import org.scalatest.matchers._

class ServerSpec extends FlatSpec with ShouldMatchers {

  behavior of "Server"

  it should "listen a specified port" in {
    val server = Server.build(Options(port = 6389))
    try {
      import com.redis._
      val r = new RedisClient("localhost", 6389)
      r.set("key", "some value") should equal(true)
      val v = r.get("key")
      v.isDefined should equal(true)
      v.get should equal("some value")
    } finally {
      server.close()
    }
  }

}
