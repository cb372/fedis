package com.github.cb372.fedis.db

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers
import com.twitter.finagle.redis.protocol._
import com.twitter.util.{Time, FuturePool}

/**
 * Author: chris
 * Created: 6/2/12
 */

trait DbTestUtils {

  def getExpiry(iterator: Iterator[(String, Entry)], key: String): Option[Time] = {
    iterator.find({
      case (k, e) => k == key
    }).flatMap({
      case (k, e) => e.expiresAt
    })
  }

  def decodeMBulkReply(reply: MBulkReply) = reply.messages.map(new String(_))
}
