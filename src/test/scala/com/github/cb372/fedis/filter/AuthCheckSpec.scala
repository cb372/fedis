package com.github.cb372.fedis.filter

import org.scalatest._
import org.scalatest.matchers._
import com.github.cb372.fedis.{Session, SessionAndCommand}
import com.twitter.finagle.Service
import com.twitter.util.Future
import com.twitter.finagle.redis.protocol.{ErrorReply, BulkReply, Reply, Get}

class AuthCheckSpec extends FlatSpec with ShouldMatchers {

  trait Fixture {
    val successReply = BulkReply("success!".getBytes)
    val mockService = new Service[SessionAndCommand, Reply] {
      def apply(request: SessionAndCommand) =
        Future.value(successReply)
    }
  }

  behavior of "AuthCheck"

  it should "deny unauthorised access to a secured server" in new Fixture {
    val authCheck = new AuthCheck(true)
    val unauthCmd = SessionAndCommand(Session(false), Get("foo"))
    val reply = authCheck.apply(unauthCmd, mockService)
    reply.get() should be (ErrorReply("ERR operation not permitted"))
  }

  it should "allow authorised access to a secured server" in new Fixture {
    val authCheck = new AuthCheck(true)
    val unauthCmd = SessionAndCommand(Session(true), Get("foo"))
    val reply = authCheck.apply(unauthCmd, mockService)
    reply.get() should be (successReply)
  }

  it should "allow unauthorised access to an unsecured server" in new Fixture {
    val authCheck = new AuthCheck(false)
    val unauthCmd = SessionAndCommand(Session(false), Get("foo"))
    val reply = authCheck.apply(unauthCmd, mockService)
    reply.get() should be (successReply)
  }

}
