package com.github.cb372.fedis.db

import java.util.concurrent.atomic.AtomicReference
import annotation.tailrec

/**
 * Author: chris
 * Created: 6/25/12
 */

trait DbState {

  /**
   * Read an immutable snapshot of the DB state, returning a result.
   * @param f reading function
   * @tparam T result type
   * @return result of the reading function
   */
  def read[T](f: Reader[T]): T

  /**
   * Process an immutable snapshot of the DB state,
   * returning a result and optionally updating the state.
   * @param f read-and-update function
   * @tparam T result type
   * @return result of the read-and-update function
   */
  def update[T](f: Updater[T]): T

}

/**
 * A lock-free thread-safe implementation of DbState.
 * The immutable state object is updated using CAS on an AtomicReference.
 * In case of contention, the losing writer simply retries until it succeeds.
 */
class AtomicRefDbState extends DbState {
  private val ref = new AtomicReference[DbContents](Map())

  def read[T](f: Reader[T]): T = f(ref.get())

  def update[T](f: Updater[T]): T = updateRec(f)

  @tailrec
  private def updateRec[T](f: Updater[T]): T = {
    val oldMap = ref.get()
    val (update, result) = f(oldMap)
    update match {
      case Some(newMap) => {
        if (ref.compareAndSet(oldMap, newMap))
          result // won CAS race, return result
        else
          updateRec(f) // lost CAS race, try again
      }
      case None => result // no update needed
    }
  }

}
