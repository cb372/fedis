package com.github.cb372.fedis
package db

import com.twitter.finagle.redis.protocol._
import com.twitter.util.FuturePool

trait KeyValueStore {
  def iterator: Iterator[(RKey, Entry)]

  def remove(key: RKey)
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
  val errNoSuchKey = ErrorReply("ERR no such key")
  val errSourceAndDestEqual = ErrorReply("ERR source and destination objects are the same")
  val errOffsetOutOfRange = ErrorReply("ERR offset is out of range")

  def errWrongNumArgs(cmdName: String) =
    ErrorReply("ERR wrong number of arguments for '%s' command".format(cmdName.toLowerCase))

}

trait DbCommon {
  protected val state: DbState
  protected val pool: FuturePool

  protected def noUpdate[R <: Reply](reply: R): (Option[DbContents], R) = (None, reply)

  protected def updateAndReply[R <: Reply](updatedMap: DbContents, reply: R): (Option[DbContents], R) =
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

  def remove(key: RKey) {
    state.update {m => (Some(m - key), Unit)}
  }

}
