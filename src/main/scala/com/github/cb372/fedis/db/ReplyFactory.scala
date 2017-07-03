package com.github.cb372.fedis.db


import com.twitter.finagle.redis.protocol.BulkReply
import com.github.cb372.fedis.util.ImplicitConversions._

/**
 * Author: chris
 * Created: 12/2/12
 */
trait ReplyFactory {
   //add by chenhj
  //def bulkReply(bytes: Array[Byte]) = BulkReply(ChannelBuffers.wrappedBuffer(bytes))
  def bulkReply(bytes: Array[Byte]) = new BulkReply(bytes)

}
