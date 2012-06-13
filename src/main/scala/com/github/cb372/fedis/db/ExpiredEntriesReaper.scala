package com.github.cb372.fedis.db

import com.twitter.util.Time

class ExpiredEntriesReaper extends KeyValueStoreTask {

  def run(dbs: Seq[KeyValueStore]) {
    dbs foreach { db =>
      reapExpiredEntries(db)
    }
  }

  private def reapExpiredEntries(db: KeyValueStore) {
    db.iterator foreach {
      case (key, value) => {
        value.expiresAt foreach { t =>
          if (t <= Time.now) db.remove(key)
        }
      }
    }
  }

}
