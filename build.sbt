organization := "com.github.cb372"

name := "fedis"

ideaProjectName := "fedis"

version := "1.1.0"

scalaVersion := "2.9.2"

crossScalaVersions := Seq("2.9.0", "2.9.1", "2.9.2")

resolvers += "Twitter Maven repo" at "http://maven.twttr.com/"

libraryDependencies ++= Seq(
    "com.twitter" % "finagle-redis" % "5.3.23"
    )

libraryDependencies ++= Seq(
    "net.debasishg" % "redisclient_2.9.1" % "2.5" % "test",
    "ch.qos.logback" % "logback-classic" % "1.0.4" % "test"
    )

libraryDependencies += "org.scalatest" %% "scalatest" % "1.7.2" % "test"

scalacOptions += "-unchecked"

publishTo := Some(Resolver.file("file",  new File( "../cb372.github.com/m2/releases" )) )
