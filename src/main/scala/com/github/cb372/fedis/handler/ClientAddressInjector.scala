package com.github.cb372.fedis.handler

import com.twitter.finagle.redis.protocol.Command
import com.github.cb372.fedis.CmdFromClient
import org.jboss.netty.channel.{UpstreamMessageEvent, MessageEvent, ChannelHandlerContext, SimpleChannelUpstreamHandler}


/**
 * Handler to convert a Command message to a CmdFromClient message.
 * Should be placed in the pipeline after the Redis codec handler.
 */
class ClientAddressInjector extends SimpleChannelUpstreamHandler {

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    val oldMsg = e.getMessage.asInstanceOf[Command]
    val newMsg = CmdFromClient(oldMsg, e.getRemoteAddress)

    ctx.sendUpstream(new UpstreamMessageEvent(e.getChannel, newMsg, e.getRemoteAddress))
  }

}
