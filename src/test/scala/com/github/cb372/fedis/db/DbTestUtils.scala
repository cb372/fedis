package com.github.cb372.fedis.db

import com.twitter.finagle.redis.protocol._
import com.twitter.util.Time
import com.twitter.finagle.redis.util.ReplyFormat
import org.jboss.netty.util.CharsetUtil

/**
 * Author: chris
 * Created: 6/2/12
 */

trait DbTestUtils {

  def getExpiry(iterator: Iterator[(RKey, Entry)], key: RKey): Option[Time] = {
    iterator.find({
      case (k, e) => k == key
    }).flatMap({
      case (k, e) => e.expiresAt
    })
  }

  def decodeMBulkReply(reply: MBulkReply): List[String] = ReplyFormat.toString(reply.messages)

  def rkey(string: String) = RKey(string.getBytes(CharsetUtil.UTF_8))

  def rkeys(strings: String*) = strings.toSeq.map(rkey(_))

}
