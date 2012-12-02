package com.github.cb372.fedis.db

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

import com.twitter.util.Time.withTimeAt
import com.twitter.util.Time
import collection.mutable.{Map => MMap}

/**
 * Author: chris
 * Created: 6/5/12
 */

class ExpiredEntriesReaperSpec extends FlatSpec with ShouldMatchers with DbTestUtils {

  trait Fixture {
    def createMap = MMap[RKey, Entry](
      rkey("expired") -> Entry(RString("a".getBytes), Some(Time.fromMilliseconds(999))),
      rkey("expiring") -> Entry(RString("b".getBytes), Some(Time.fromMilliseconds(1000))),
      rkey("not yet expired") -> Entry(RString("c".getBytes), Some(Time.fromMilliseconds(1001))),
      rkey("no expiry") -> Entry(RString("d".getBytes), None)
    )
    def toDb(map: MMap[RKey, Entry]) = new KeyValueStore {
      def iterator = map.iterator
      def remove(key: RKey) {
        map.remove(key)
      }
    }
  }

  behavior of "ExpiredEntriesReaper"

  it should "reap entries that have expired or are expiring right now" in new Fixture {
    val reaper = new ExpiredEntriesReaper
    val map = createMap
    withTimeAt(Time.fromMilliseconds(1000)){_ => reaper.run(Seq(toDb(map)))}
    map.keySet should not contain (rkey("expired"))
    map.keySet should not contain (rkey("expiring"))
  }

  it should "not reap entries that have not yet expired" in new Fixture {
    val reaper = new ExpiredEntriesReaper
    val map = createMap
    withTimeAt(Time.fromMilliseconds(1000)){_ => reaper.run(Seq(toDb(map)))}
    map.keySet should contain (rkey("not yet expired"))
  }

  it should "not reap entries that have no expiry" in new Fixture {
    val reaper = new ExpiredEntriesReaper
    val map = createMap
    withTimeAt(Time.fromMilliseconds(1000)){_ => reaper.run(Seq(toDb(map)))}
    map.keySet should contain (rkey("no expiry"))
  }
}
