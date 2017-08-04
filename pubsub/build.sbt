lazy val commonSettings = Seq(
  organization := "com.google.cloud.spark",
  version := "0.1.0",
  scalaVersion := "2.11.8",
  crossScalaVersions := Seq("2.10.5"),
  sparkVersion := "2.0.2",
  name := "spark-pubsub",
  spName := "google/spark-pubsub",
  sparkComponents ++= Seq("core", "streaming"),
  mainClass := Some("com.google.cloud.spark.pubsub.examples.MinuteRideAggregator")
)

lazy val shaded = (project in file(".")).
  settings(commonSettings: _*).
  settings(
  )
assemblyOption in assembly := (assemblyOption in assembly).value.copy(
  includeScala = false)

val excludedOrgs = Seq(
  "aopalliance",
  "com.google.code.findbugs",
  "com.fasterxml.jackson.core",
  "com.thoughtworks.paranamer",
  "joda-time",
  "org.apache.commons",
  "org.apache.httpcomponents",
  "org.apache.velocity",
  "org.codehaus.jackson",
  "org.mortbay.jetty",
  "org.slf4j",
  "org.xerial.snappy"
)
libraryDependencies ++= Seq(
  "com.google.cloud" % "google-cloud-pubsub" % "0.17.2-alpha")
  .map(_.excludeAll(excludedOrgs.map(ExclusionRule(_)): _*))

libraryDependencies ++= Seq(
  "com.google.cloud" % "google-cloud-monitoring" % "0.17.2-alpha")
  .map(_.excludeAll(excludedOrgs.map(ExclusionRule(_)): _*))

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

val myPackage = "com.google.cloud.spark.pubsub"
val relocationPrefix = s"$myPackage.shaded"
val renamed = Seq(
  "io.netty",
  "autovalue",
  "com.google",
  "javax.inject",
  "javax.jdo",
  "javax.servlet",
  "org.json",
  "org.threeteen")
val notRenamed = Seq(
  // sbt-assembly cannot handle services registry
  "io.grpc",
  // (netty tcnative)
  "org.apache.tomcat",
  myPackage,
  // Exposed to the user in the API.
  "com.google.pubsub")

assemblyShadeRules in assembly := (
  // Rename preserved prefixes to themselves first to keep them unchanged
  notRenamed.map(prefix => ShadeRule.rename(s"$prefix**" -> s"$prefix@1"))
    ++: renamed.map(prefix => ShadeRule.rename(s"$prefix**" -> s"$relocationPrefix.$prefix@1"))
  ).map(_.inAll)

assemblyMergeStrategy in assembly := {
  case PathList(ps @ _*) if ps.last.endsWith(".properties") => MergeStrategy.filterDistinctLines
  case PathList(ps @ _*) if ps.last.endsWith(".proto") => MergeStrategy.discard
  case x => (assemblyMergeStrategy in assembly).value(x)
}
