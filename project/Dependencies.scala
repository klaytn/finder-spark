package spark_project
import sbt._

object Dependencies {

  val spark = Seq(
    "org.apache.spark" %% "spark-yarn"      % Versions.spark,
    "org.apache.spark" %% "spark-core"      % Versions.spark,
    "org.apache.spark" %% "spark-sql"       % Versions.spark,
    "org.apache.spark" %% "spark-mllib"     % Versions.spark,
    "org.apache.spark" %% "spark-streaming" % Versions.spark,
    "org.apache.spark" %% "spark-hive"      % Versions.spark
  )

  val sparkProvided = Seq(
    "org.apache.spark" %% "spark-yarn"      % Versions.spark % "provided",
    "org.apache.spark" %% "spark-core"      % Versions.spark % "provided",
    "org.apache.spark" %% "spark-sql"       % Versions.spark % "provided",
    "org.apache.spark" %% "spark-mllib"     % Versions.spark % "provided",
    "org.apache.spark" %% "spark-streaming" % Versions.spark % "provided",
    "org.apache.spark" %% "spark-hive"      % Versions.spark % "provided"
  )

  val circe = Seq(
    "io.circe" %% "circe-core",
    "io.circe" %% "circe-generic",
    "io.circe" %% "circe-parser"
  ).map(_ % Versions.circe)

  val json4s = Seq(
    "org.json4s" %% "json4s-core",
    "org.json4s" %% "json4s-native"
  ).map(_ % Versions.json4s)

  val jackson = Seq(
    "com.fasterxml.jackson.core"   % "jackson-core"          % "2.10.2",
    "com.fasterxml.jackson.core"   % "jackson-databind"      % "2.10.2",
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.10.2"
  )
}
