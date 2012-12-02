package com.github.cb372.fedis.util

import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.util.CharsetUtil
import com.github.cb372.fedis.db.RKey

/**
 * Author: chris
 * Created: 12/2/12
 */
private[fedis] object ImplicitConversions {

  /*
  * Implicit conversions to deal with this API change:
  * https://github.com/twitter/finagle/commit/5601713f0c9515da53fe3ade203ce94aeb49b77f
  *
  * The Command case classes used to hold Strings or Array[Byte]s, but now hold ChannelBuffers.
  * We are not interested in the performance gains from using ChannelBuffers directly,
  * so we just convert them to whatever is convenient.
  */

  implicit def channelBufferToByteArray(cb: ChannelBuffer): Array[Byte] = {
    val bytes = new Array[Byte](cb.readableBytes())
    cb.getBytes(cb.readerIndex(), bytes, 0, bytes.length)
    bytes
  }

  implicit def channelBufferToString(cb: ChannelBuffer): String =
    new String(channelBufferToByteArray(cb), CharsetUtil.UTF_8)

  implicit def channelBufferToRKey(cb: ChannelBuffer): RKey =
    RKey(channelBufferToByteArray(cb))

  implicit def convertMap(kv: Map[ChannelBuffer, ChannelBuffer]): Map[RKey, Array[Byte]] = {
    kv.map {
      case (k: ChannelBuffer, v: ChannelBuffer) => channelBufferToRKey(k) -> channelBufferToByteArray(v)
    }
  }

}
