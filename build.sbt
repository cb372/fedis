name := "fedis"

ideaProjectName := "fedis"

version := "0.1"

scalaVersion := "2.9.1"

resolvers += "Twitter Maven repo" at "http://maven.twttr.com/"

libraryDependencies += "com.twitter" % "finagle-redis" % "4.0.5"

libraryDependencies += "org.scalatest" %% "scalatest" % "1.7.2" % "test"

scalacOptions += "-unchecked"