package com.github.cb372.fedis

/**
 * Author: chris
 * Created: 12/1/12
 */
package object db {

  /**
   * The contents of a Redis DB. A key-value store with byte array keys mapping to `Entry`s.
   */
  type DbContents = Map[RKey, Entry]

  /**
   * A function that reads a value from a DB.
   * @tparam T return type
   */
  type Reader[T] = DbContents => T

  /**
   * A function that reads a value and optionally updates the DB.
   * @tparam T return type
   */
  type Updater[T]  = DbContents => (Option[DbContents], T)

}
