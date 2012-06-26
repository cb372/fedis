# Fedis

A mock [Redis](http://redis.io/) server built with [Finagle](https://github.com/twitter/finagle).

Note that Fedis stores all data in-memory. Nothing is persisted to disk.

## How to use

    import com.github.cb372.fedis.Server

    // Start a server, passing in a few configuration options
    val server = Server.build(Options(port = 6379,
                                      serverPassword = Some("secret"),
                                      threadPoolSize = 10))

    // ... do something ...

    // Don't forget to shut down the server once you're done with it
    server.close()

## Supported commands

See the [Wiki page](https://github.com/cb372/fedis/wiki/Supported-Redis-Commands).

## Restrictions

* Redis supports 64 bit unsigned integers, but Fedis only supports 32 bit integers. (See [here](https://github.com/twitter/finagle/pull/87) for the reason.)
