import sbtassembly.MergeStrategy

lazy val commonSettings = Seq(
  organization := "com.google.cloud.bigdataoss",
  name := "spark-pubsub",
  version := "0.1.0-SNAPSHOT",
  scalaVersion := "2.11.8",
  crossScalaVersions := Seq("2.10.5"),
  sparkVersion := "2.2.0",
  spName := "google/spark-pubsub",
  sparkComponents ++= Seq("core", "streaming")
)


def itFilter(name: String): Boolean = name endsWith "ITSuite"
def unitFilter(name: String): Boolean = (name endsWith "Suite") && !itFilter(name)
// Default IntegrationTest config uses separate test directory, build files
lazy val ITest = config("it") extend(Test)

lazy val shaded = (project in file("."))
  .configs(ITest)
  .settings(
    commonSettings,
    inConfig(ITest)(Defaults.testTasks),
    testOptions in Test := Seq(Tests.Filter(unitFilter)),
    testOptions in ITest := Seq(Tests.Filter(itFilter)))

assemblyOption in assembly := (assemblyOption in assembly).value.copy(
  includeScala = false)

parallelExecution in ITest := false

val excludedOrgs = Seq(
  "com.google.auto.value",
  "com.google.code.findbugs",
  "com.fasterxml.jackson.core",
  "joda-time",
  "org.apache.httpcomponents"
)

val googleCloudJavaVersion = "0.32.0"
val hadoopVersion = "2.8.0"
libraryDependencies ++= (
  Seq(
    "com.google.cloud" % "google-cloud-pubsub" % s"$googleCloudJavaVersion-beta",
    "com.google.cloud" % "google-cloud-monitoring" % s"$googleCloudJavaVersion-beta",
    "org.scalatest" %% "scalatest" % "3.0.1" % "test",
    "org.mockito" % "mockito-all" % "1.10.19" % "test")
    .map(_.excludeAll(excludedOrgs.map(ExclusionRule(_)): _*))
  ++ Seq(
    // Fix for [HADOOP-10961] in tests
    "org.apache.hadoop" % "hadoop-common" % hadoopVersion % "provided",
    "org.apache.hadoop" % "hadoop-mapreduce-client-core" % hadoopVersion % "provided")
    .map(_.excludeAll(Seq("com.google.guava", "com.google.protobuf").map(ExclusionRule(_)): _*)))


val myPackage = "com.google.cloud.spark.pubsub"
val relocationPrefix = s"$myPackage.repackaged"
val renamed = Seq(
  "io.netty",
  "io.opencensus",
  "com.google",
  "org.json",
  "org.threeten")
val notRenamed = Seq(
  // sbt-assembly cannot handle services registry
  "io.grpc",
  myPackage,
  // Exposed to the user in the API.
  "com.google.cloud.pubsub")

assemblyShadeRules in assembly := (
  // Rename preserved prefixes to themselves first to keep them unchanged
  notRenamed.map(prefix => ShadeRule.rename(s"$prefix**" -> s"$prefix@1"))
    ++: renamed.map(prefix => ShadeRule.rename(s"$prefix**" -> s"$relocationPrefix.$prefix@1"))
  ).map(_.inAll)

assemblyMergeStrategy in assembly := {
  case PathList(ps @ _*) if ps.last.endsWith(".properties") => MergeStrategy.filterDistinctLines
  case PathList(ps @ _*) if ps.last.endsWith(".proto") => MergeStrategy.discard
  case PathList(ps @ _*) if ps.last.startsWith("libnetty_tcnative")
    // This is an abuse of MergeStrategy to relocate netty-tcnative's .so
      => new MergeStrategy() {
    val name = "netty-tcnative"
    def apply(tempDir: File, path: String, files: Seq[File]) =
      Right(files.map(f => f -> path.replace(
        "libnetty", s"lib${relocationPrefix.replace('.', '_')}_netty")))
  }
  case x => (assemblyMergeStrategy in assembly).value(x)
}

fork := true
