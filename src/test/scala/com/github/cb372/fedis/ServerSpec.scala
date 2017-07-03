package com.github.cb372.fedis

import org.scalatest._
import org.scalatest.matchers._

class ServerSpec extends FlatSpec with Matchers {

  behavior of "Server"

  it should "listen a specified port" in {
    val server = Server.build(Options(port = 6389))
    try {
      import com.redis._
      val r = new RedisClient(host="127.0.0.1",port = 6379,secret = Some("123456"))
      r.set("key", "some value") should equal(true)
      val v = r.get("key")
      v.isDefined should equal(true)
      v.get should equal("some value")
    }catch {
      case e:Exception =>
          e.printStackTrace()
    } finally{
      server.close()
    }
  }

}
