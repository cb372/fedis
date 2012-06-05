package com.github.cb372.fedis.db

/**
 * Author: chris
 * Created: 6/2/12
 */

trait KeyValueStoreTask {
  def run(dbs: Seq[KeyValueStore])
}

class DummyKeyValueStoreTask extends KeyValueStoreTask {
  def run(dbs: Seq[KeyValueStore]) {}
}
