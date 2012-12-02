package com.github.cb372.fedis.db

import org.scalatest.matchers.{MatchResult, Matcher}
import com.twitter.finagle.redis.protocol.{BulkReply, Reply}
import com.twitter.finagle.redis.util.CBToString

/**
 * Author: chris
 * Created: 12/2/12
 */
trait DbMatchers {

  class BulkReplyMatcher(right: String) extends Matcher[Reply] {
    def apply(left: Reply) = {

      if (left.isInstanceOf[BulkReply]) {
        val content = CBToString(left.asInstanceOf[BulkReply].message)
        MatchResult(
          content == right,
          "The reply's content was [" + content + "], expected [" + right + "]",
          "The reply's content was " + right
        )
      } else {
        MatchResult(
          false,
          "The reply was a " + left.getClass.getSimpleName + ", expected a BulkReply",
          "The reply was a BulkReply"
        )
      }

    }
  }

  def beBulkReplyWithValue(value: String) = new BulkReplyMatcher(value)

}

object DbMatchers extends DbMatchers
