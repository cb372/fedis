package com.github.cb372.fedis
package db

import com.twitter.finagle.redis.protocol._
import com.twitter.util.{Time, FuturePool}
import com.twitter.conversions.time._

trait KeyValueStore {
  def iterator: Iterator[(String, Entry)]

  def remove(key: String)
}

object DbConstants {
  private[db] val zeroByte = 0.toByte
  private[db] def nil = new Array[Byte](0)
}

object Replies {
  val ok = StatusReply("OK")

  val errWrongType = ErrorReply("ERR Operation against a key holding the wrong kind of value")
  val errIntOverflow = ErrorReply("ERR increment or decrement would overflow")
  val errNotAnInt = ErrorReply("ERR value is not an integer or out of range")
  val errNotABit = ErrorReply("ERR bit is not an integer or out of range")

  def errWrongNumArgs(cmdName: String) =
    ErrorReply("ERR wrong number of arguments for '%s' command".format(cmdName.toLowerCase))

}

trait DbCommon {
  protected val state: DbState
  protected val pool: FuturePool

  protected def noUpdate[R <: Reply](reply: R): (Option[Map[String, Entry]], R) = (None, reply)

  protected def updateAndReply[R <: Reply](updatedMap: Map[String, Entry], reply: R): (Option[Map[String, Entry]], R) =
    (Some(updatedMap), reply)

}

class Db(val pool: FuturePool)
  extends KeyValueStore
  with DbCommon
  with KeysOps
  with StringsOps
  with HashesOps {

  protected val state: DbState = new AtomicRefDbState

  def iterator = state.read(_.iterator)

  def remove(key: String) {
    state.update {m => (Some(m - key), Unit)}
  }

}
