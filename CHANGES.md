CHANGELOG
=========

v1.2.0 (15 December 2012)
--------------------

* Support for Redis lists and sets
    * Supported list commands: LLEN, LPUSH, LPOP, RPUSH, RPOP
    * Supported set commands: SCARD, SADD, SREM, SISMEMBER, SMEMBERS
* Add HKEYS command
* Fix issue #2
* Upgrade finagle-redis dependency to 5.3.23.

v1.1.0 (25 July 2012)
---------------------

* Support 64 bit integers (see https://github.com/twitter/finagle/pull/87 for details)

* Upgrade finagle-redis dependency to 5.3.1.

* Add cross-building for all Scala 2.9.x versions.

v1.0.0 (3 July 2012)
--------------------

First release.
