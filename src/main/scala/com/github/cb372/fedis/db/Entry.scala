package com.github.cb372.fedis.db

import com.twitter.util.Time

/**
 * Author: chris
 * Created: 6/5/12
 */

trait HasExpiry {
  val expiresAt: Option[Time]
}

case class Entry(value: RVal, expiresAt: Option[Time] = None) extends HasExpiry

