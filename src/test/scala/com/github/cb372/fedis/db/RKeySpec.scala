package com.github.cb372.fedis.db

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

/**
 * Author: chris
 * Created: 6/13/12
 */

class RKeySpec extends FlatSpec with ShouldMatchers {

  behavior of "RKey"

  it should "have the same equality semantics as a String" in {
    val key1 = RKey("foo".getBytes)
    val key2 = RKey(("f" + "o" + "od".substring(0, 1)).getBytes)
    val key3 = RKey("bar".getBytes)

    key1 should equal(key1)
    key2 should equal(key2)
    key3 should equal(key3)

    key1 should equal(key2)
    key1 should not equal(key3)

    key2 should equal(key1)
    key2 should not equal(key3)

    key3 should not equal(key1)
    key3 should not equal(key2)
  }

  it should "work correctly as a key in a map" in {
    val m: Map[RKey, String] = Map(
      RKey("abc".getBytes) -> "foo",
      RKey("def".getBytes) -> "bar"
    )
    m.contains(RKey("abc".getBytes)) should equal(true)
    m.contains(RKey("def".getBytes)) should equal(true)
    m.contains(RKey("ghi".getBytes)) should equal(false)
    m(RKey("abc".getBytes)) should equal("foo")
  }

}
