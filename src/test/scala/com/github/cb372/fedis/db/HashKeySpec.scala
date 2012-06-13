package com.github.cb372.fedis.db

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

/**
 * Author: chris
 * Created: 6/13/12
 */

class HashKeySpec extends FlatSpec with ShouldMatchers {

  behavior of "HashKey"

  it should "work correctly as a key in a map" in {
    val m: Map[HashKey, String] = Map(
      HashKey("abc".getBytes) -> "foo",
      HashKey("def".getBytes) -> "bar"
    )
    m.contains(HashKey("abc".getBytes)) should equal(true)
    m.contains(HashKey("def".getBytes)) should equal(true)
    m.contains(HashKey("ghi".getBytes)) should equal(false)
    m(HashKey("abc".getBytes)) should equal("foo")
  }

}
