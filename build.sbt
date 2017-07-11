import sbt.Credentials._

organization := "com.github.cb372"

name := "fedis"

//ideaProjectName := "fedis"

version := "1.3.0"

scalaVersion := "2.11.8"

crossScalaVersions := Seq("2.9.0", "2.9.1", "2.9.2","2.11.8")

resolvers += "Twitter Maven repo" at "http://maven.twttr.com/"

libraryDependencies ++= Seq(
    "com.twitter" %% "finagle-redis" % "6.35.0" ,
    "com.twitter" % "util-codec_2.11" % "6.35.0"
    )

libraryDependencies ++= Seq(
    "net.debasishg" % "redisclient_2.11" % "3.4" % "test",
    "ch.qos.logback" % "logback-classic" % "1.0.4" % "test"
    )


libraryDependencies +="org.scalatest" % "scalatest_2.11" % "2.1.7" % "test"

scalacOptions += "-unchecked"

//publishTo := Some(Resolver.file("file",  new File( "../cb372.github.com/m2/releases" )) )
//publishTo := Some(Resolver.file("file",  new File( "../cb372.github.com/m2/releases" )) )
//http://106.120.210.50:8081/nexus/content/groups/public/

/*publishTo := Some("Sonatype Snapshots Nexus" at "http://106.120.210.50:8081/nexus/content/repositories/releases")*/

publishMavenStyle := true
pomIncludeRepository := { _ => false }

publishTo := {
  val nexus = "http://106.120.210.50:8081/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "nexus/content/repositories/snapshots")
  else
  //  Some("releases"  at nexus + "nexus/content/repositories/releases")
    Some("releases"  at nexus + "nexus/service/local/staging/deploy/maven2")
}

credentials += Credentials("Sonatype Nexus Repository Manager", "106.120.210.50:8081", "admin", "chen_123qwe")
//Credentials.add("Sonatype Nexus Repository Manager", "106.120.210.50:8081", "admin","chen_123qwe")


