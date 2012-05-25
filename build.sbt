name := "finagle-redis-server"

ideaProjectName := "finagle-redis-server"

version := "0.1"

scalaVersion := "2.9.1"

resolvers += "Twitter Maven repo" at "http://maven.twttr.com/"

// Force the util-* versions to 4.0.2 because 4.0.1 is missing from Maven repo
libraryDependencies ++= Seq(
    "com.twitter" % "finagle-redis" % "4.0.2",
    "com.twitter" % "util-collection" % "4.0.2",
    "com.twitter" % "util-core" % "4.0.2",
    "com.twitter" % "util-hashing" % "4.0.2",
    "com.twitter" % "util-logging" % "4.0.2"
)

libraryDependencies += "org.scalatest" %% "scalatest" % "1.7.2" % "test"

scalacOptions += "-unchecked"