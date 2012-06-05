package com.github.cb372.fedis.db

/**
 * Author: chris
 * Created: 6/2/12
 */

trait DbTask {
  def run(dbs: Seq[Db])
}

class DummyDbTask extends DbTask {
  def run(dbs: Seq[Db]) {}
}
