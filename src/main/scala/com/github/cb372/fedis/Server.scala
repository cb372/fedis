package com.github.cb372.fedis

import db.ExpiredEntriesReaper
import service.RedisService
import codec.RedisServerCodec
import filter.{AuthCheck, SessionManagement}
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.builder.{ServerBuilder, Server => FinagleServer}
import com.twitter.util.{Command => _, _}
import java.io.Closeable
import java.net.{InetSocketAddress, SocketAddress}
import java.util.concurrent.Executors

case class Options(port: Int = 6379,
                   serverPassword: Option[String] = None,
                   threadPoolSize: Int = 4)

object Server {

  def build(options: Options): FinagleServer = {
    val resources = new Resources(options.threadPoolSize)

    val sessionMgmt = new SessionManagement(options.serverPassword)
    val authCheck = new AuthCheck(options.serverPassword.isDefined)
    val reaper = new ExpiredEntriesReaper
    val redis = new RedisService(resources.futurePool, resources.timer, reaper)

    val myService = sessionMgmt andThen authCheck andThen redis

    val server = ServerBuilder()
      .codec(RedisServerCodec())
      .bindTo(new InetSocketAddress(options.port))
      .name("redisserver")
      .build(myService)

    new ResourceTidyingServerWrapper(server, resources)
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

  private def printUsage() {
    println("Usage: %s [port [password]]".format(Server.getClass.getName))
  }

  private class Resources(threadPoolSize: Int) extends Closeable {
    val threadPool = Executors.newFixedThreadPool(threadPoolSize)
    val futurePool = FuturePool(threadPool)

    val timer = new JavaTimer(false)

    def close() {
      try {
        threadPool.shutdownNow()
      } finally {
        timer.stop()
      }
    }
  }

  /*
   * A decorator for a finagle Server that cleans up
   * any necessary resources after the server is closed.
   */
  private class ResourceTidyingServerWrapper(base: FinagleServer,
                                             resources: Resources) extends FinagleServer {
     // def localAddress  = base.boundAddress

    /*override def close(timeout: Duration) {
      try {
        base.close(timeout)
      } finally {
        resources.close()
      }
    }*/

    override def boundAddress: SocketAddress = base.boundAddress

    override protected def closeServer(deadline: Time): Future[Unit] = ???


    override def ready(timeout: Duration)(implicit permit: Awaitable.CanAwait): ResourceTidyingServerWrapper.this.type = ???


    override def result(timeout: Duration)(implicit permit: Awaitable.CanAwait): Unit =base.result(timeout)


    override def isReady(implicit permit: Awaitable.CanAwait): Boolean = base.isReady


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



