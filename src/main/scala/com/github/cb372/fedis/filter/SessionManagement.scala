package com.github.cb372.fedis
package filter

import collection.mutable.{Map => MMap}
import java.net.SocketAddress
import com.twitter.finagle.{Service, Filter}
import com.twitter.finagle.redis.protocol._
import com.twitter.util.Future

/**
 * Filter to map a client remote address to a Redis session.
 * Handles the following commands without passing them on to the service:
 *   - AUTH
 *   - SELECT
 *   - QUIT
 *
 * @param serverPassword The password for this Redis server, if it is password-protected
 */
class SessionManagement(serverPassword: Option[String]) extends Filter[CmdFromClient, Reply, SessionAndCommand, Reply] {
  type Key = SocketAddress

  private val sessions = MMap[Key, Session]()

  def apply(request: CmdFromClient, service: Service[SessionAndCommand, Reply]) = {
    val key = request.clientAddr
    val session = sessions.getOrElseUpdate(key, {
      Session()
    })
    request.cmd match {
      case Auth(code) => auth(key, session, code)
      case Select(index) => select(key, session, index)
      case Quit() => quit(key)
      case _ => {
        // pass all other commands onto service, along with the session
        service(SessionAndCommand(session, request.cmd))
      }
    }
  }

  private def auth(key: Key, session: Session, code: String): Future[Reply] =
    serverPassword match {
      case Some(pword) => {
        if (code == pword) {
          // correct password

          // mark session as authorized
          sessions.put(key, session.copy(authorized = true))

          Future.value(StatusReply("OK"))
        } else {
          // incorrect password

          // mark session as unauthorized
          sessions.put(key, session.copy(authorized = false))

          Future.value(ErrorReply("ERR invalid password"))
        }
      }
      case None => {
        // this server does not have a password set, so just reply OK
        Future.value(StatusReply("OK"))
      }
    }

  private def select(key: Key, session: Session, index: Int): Future[Reply] =
    if (index < 0 || index > Constants.maxDbIndex) {
      Future.value(ErrorReply("ERR invalid DB index"))
    } else {
      // update session
      sessions.put(key, session.copy(db = index))

      Future.value(StatusReply("OK"))
    }

  private def quit(key: Key): Future[Reply] = {
    // remove session
    sessions.remove(key)

    Future.value(StatusReply("OK"))
  }

}
