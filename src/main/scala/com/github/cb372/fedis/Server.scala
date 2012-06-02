package com.github.cb372.fedis

import service.RedisService
import codec.RedisServerCodec
import filter.{AuthCheck, SessionManagement}
import com.twitter.finagle.redis._
import protocol._
import java.net.{SocketAddress, InetSocketAddress}
import com.twitter.finagle.builder.{Server => FinagleServer, ServerBuilder}
import com.twitter.util.{Duration, FuturePool}
import java.util.concurrent.{ExecutorService, Executors}

case class Options(port: Int = 6379,
                   serverPassword: Option[String] = None,
                   threadPoolSize: Int = 4)

object Server {

  def build(options: Options): FinagleServer = {
    val threadPool = Executors.newFixedThreadPool(options.threadPoolSize)
    val futurePool = FuturePool(threadPool)

    val sessionMgmt = new SessionManagement(options.serverPassword)
    val authCheck = new AuthCheck(options.serverPassword.isDefined)
    val redis = new RedisService(futurePool)

    val myService = sessionMgmt andThen authCheck andThen redis

    val server = ServerBuilder()
      .codec(RedisServerCodec())
      .bindTo(new InetSocketAddress(options.port))
      .name("redisserver")
      .build(myService)

    new ResourceTidyingServerWrapper(server, threadPool)
  }


  def main(args: Array[String]) {
    val options = args.toList match {
      case port :: password :: threads :: Nil => Some(Options(port.toInt, Some(password), threads.toInt))
      case port :: password :: Nil => Some(Options(port.toInt, Some(password)))
      case port :: Nil => Some(Options(port.toInt))
      case Nil => Some(Options())
      case _ => None
    }

    options match {
      case Some(o) => build(o)
      case _ => printUsage()
    }
  }

  private def printUsage() =
    println("Usage: %s [port [password]]".format(Server.getClass.getName))

  /*
   * A decorator for a finagle Server that cleans up
   * any necessary resources after the server is closed.
   */
  private class ResourceTidyingServerWrapper(base: FinagleServer,
                                             threadPool: ExecutorService) extends FinagleServer {
    def localAddress = base.localAddress

    def close(timeout: Duration) {
      base.close(timeout)
      cleanupResources()
    }

    private def cleanupResources() {
      threadPool.shutdown()
    }
  }
}

object Constants {

  // The maximum DB index that can be SELECTed
  val maxDbIndex = 15
  val numDbs = maxDbIndex + 1 // because DBs are zero-indexed

}

case class Session(authorized: Boolean = false, db: Int = 0)

case class SessionAndCommand(session: Session, cmd: Command)

case class CmdFromClient(cmd: Command, clientAddr: SocketAddress)



