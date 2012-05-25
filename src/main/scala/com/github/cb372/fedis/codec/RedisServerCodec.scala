package com.github.cb372.fedis
package codec

import handler.ClientAddressInjector

import com.twitter.naggati.{Codec => NaggatiCodec}
import com.twitter.finagle.{Codec, ServerCodecConfig, CodecFactory}
import com.twitter.finagle.redis.protocol.{ReplyCodec, CommandCodec, Reply}
import org.jboss.netty.channel.{Channels, ChannelPipelineFactory}


object RedisServerCodec {
  def apply() = new RedisServerCodec
  def get() = apply()
}

class RedisServerCodec extends CodecFactory[CmdFromClient, Reply]#Server {
  def apply(config: ServerCodecConfig) =
    new Codec[CmdFromClient, Reply] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline = {
          val pipeline = Channels.pipeline()
          val commandCodec = new CommandCodec
          val replyCodec = new ReplyCodec

          pipeline.addLast("codec", new NaggatiCodec(commandCodec.decode, replyCodec.encode))
          pipeline.addLast("clientAddr", new ClientAddressInjector)

          pipeline
        }
      }
    }

}
