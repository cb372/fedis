package com.github.cb372.fedis.db

import com.twitter.finagle.redis.protocol.BulkReply
import org.jboss.netty.buffer.ChannelBuffers

/**
 * Author: chris
 * Created: 12/2/12
 */
trait ReplyFactory {

  def bulkReply(bytes: Array[Byte]) = BulkReply(ChannelBuffers.wrappedBuffer(bytes))

}
