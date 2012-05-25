package com.github.cb372.fedis
package filter

import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.finagle.redis.protocol.{ErrorReply, Reply}
import com.twitter.util.Future

/**
 * Filter to check that a client's session is authorized.
 * @param authRequired Is this a password-protected Redis server?
 *                     If not, this filter does nothing except forward requests.
 *
 */
class AuthCheck(authRequired: Boolean) extends SimpleFilter[SessionAndCommand, Reply] {
  def apply(request: SessionAndCommand, service: Service[SessionAndCommand, Reply]) = {
    if (authRequired && !request.session.authorized)
      Future.value(ErrorReply("ERR operation not permitted"))
    else
      service(request)
  }
}
