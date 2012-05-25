package com.github.cb372.fedis

import service.RedisService
import codec.RedisServerCodec
import filter.{AuthCheck, SessionManagement}
import collection.mutable.{Map => MMap}
import com.twitter.finagle.redis._
import protocol._
import java.net.{SocketAddress, InetSocketAddress}
import com.twitter.finagle.builder.ServerBuilder


object Server {

  def build(port: Int = 6379, serverPassword: Option[String] = None) {
    val sessionMgmt = new SessionManagement(serverPassword)
    val authCheck = new AuthCheck(serverPassword.isDefined)
    val redis = new RedisService

    val myService = sessionMgmt andThen authCheck andThen redis

    ServerBuilder()
      .codec(RedisServerCodec())
      .bindTo(new InetSocketAddress(6379))
      .name("redisserver")
      .build(myService)
    }

  def main(args: Array[String]) {
    args.toList match {
      case port :: password :: Nil => build(port.toInt, Some(password))
      case port :: Nil => build(port.toInt)
      case Nil => build()
      case _ => printUsage
    }
  }

  private def printUsage =
    println("Usage: %s [port [password]]".format(Server.getClass.getName))
}

object Constants {

  // The maximum DB index that can be SELECTed
  val maxDbIndex = 15

}

case class Session(authorized: Boolean = false, db: Int = 0)

case class SessionAndCommand(session: Session, cmd: Command)

case class CmdFromClient(cmd: Command, clientAddr: SocketAddress)



