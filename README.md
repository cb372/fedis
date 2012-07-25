# Fedis

A mock [Redis](http://redis.io/) server built with [Finagle](https://github.com/twitter/finagle).

Fedis's main use is as a drop-in replacement for a Redis server in functional/system tests.

Note that Fedis stores all data in-memory. Nothing is persisted to disk.

Benefits:

* Isolation of tests: Run tests in parallel, each talking to its own Fedis server, with no fear of tests interfering with each other's data.

* Easy teardown: When your test is complete, just shut down Fedis. Nothing to cleanup.

## How to use

    import com.github.cb372.fedis.Server

    // Start a server, passing in a few configuration options
    val server = Server.build(Options(port = 6379,
                                      serverPassword = Some("secret"),
                                      threadPoolSize = 10))

    // ... do something ...

    // Don't forget to shut down the server once you're done with it
    server.close()

## Download

* Using Maven:

        <dependency>
            <groupId>com.github.cb372</groupId>
            <artifactId>fedis_2.9.2</artifactId>
            <version>1.1.0</version>
        </dependency>
        
        <repository>
          <id>cb372</id>
          <name>Chris Birchall's Maven repo</name>
          <url>http://cb372.github.com/m2/releases</url>
        </repository>

* Using SBT:

        resolvers += "Chris Birchall's Maven repo" at "http://cb372.github.com/m2/releases"
        libraryDependencies += "com.github.cb372" %% "fedis" % "1.1.0"

## Supported commands

See the [Wiki page](https://github.com/cb372/fedis/wiki/Supported-Redis-Commands).

## Restrictions

* Performance: Don't expect the blazing speed of Redis!
