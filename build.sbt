name := "fedis"

ideaProjectName := "fedis"

version := "0.1"

scalaVersion := "2.9.1"

resolvers += "Twitter Maven repo" at "http://maven.twttr.com/"

// Twitter Maven artifacts are a huge mess,
// so we list each dependency separately and hope that everything works together.
// Note that finagle-redis is the normal (Scala 2.8) variety,
// whilst all other libs are the _2.9.1 variety.

libraryDependencies ++= Seq(
    "com.twitter" % "finagle-redis" % "4.0.5" intransitive(),
    "com.twitter" %% "finagle-core" % "4.0.2",
    "com.twitter" %% "naggati" % "2.2.3",
    "com.twitter" %% "util-logging" % "4.0.1" % "runtime"
    )

libraryDependencies ++= Seq(
    "net.debasishg" %% "redisclient" % "2.5" % "test",
    "ch.qos.logback" % "logback-classic" % "1.0.4" % "test"
    )

libraryDependencies += "org.scalatest" %% "scalatest" % "1.7.2" % "test"

scalacOptions += "-unchecked"

publishTo := Some(Resolver.file("file",  new File( "../cb372.github.com/m2/releases" )) )
