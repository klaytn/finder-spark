package spark_project
import org.scalafmt.sbt.ScalafmtPlugin.autoImport._
import sbt.Keys._
import sbt.{Resolver, _}
import sbtassembly.AssemblyPlugin.autoImport._

object Projects {
  val commonSettings = Seq(
    scalaVersion := Versions.scala,
    assembly / assemblyMergeStrategy := {
      case "application.conf"            => MergeStrategy.concat
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x                             => MergeStrategy.first
    },
    assembly / assemblyShadeRules ++= Seq(
      ShadeRule
        .rename("okhttp3.**" -> "shade.okhttp3.@1")
        .inLibrary("com.squareup.okhttp3" % "okhttp" % "4.9.0")
        .inProject,
      ShadeRule
        .rename("okio.**" -> "shade.okio.@1")
        .inLibrary("com.squareup.okio" % "okio" % "2.8.0")
        .inProject,
      ShadeRule
        .rename("okhttp3.**" -> "shade.okhttp3.@1")
        .inLibrary("org.web3j" % "core" % "4.8.8")
        .inProject,
      ShadeRule
        .rename("okio.**" -> "shade.okio.@1")
        .inLibrary("org.web3j" % "core" % "4.8.8")
        .inProject
    ),
    assembly / test := {}, // Set to skip tests when creating fatjars. If tests are needed, they are explicitly invoked by CI.
    resolvers ++= Seq(
      Resolver.mavenLocal,
      Resolver.mavenCentral,
      Repository.cloudera,
      Repository.sonatype,
      Repository.twitter,
//      Repository.klaytnSnapshot,
//      Repository.klaytnRelease,
      Repository.spring,
      Repository.jitpack
    ),
    libraryDependencies ++= Seq(
      "joda-time"     % "joda-time"  % Versions.jodaTime,
      "org.scalatest" %% "scalatest" % Versions.scalatest % Test,
      "org.scalamock" %% "scalamock" % Versions.scalamock % Test,
    ),
    scalacOptions ++= Seq(
      "-deprecation",
      "-encoding",
      "UTF-8",
      "-feature",
      "-Ywarn-unused-import",
      "-Ywarn-dead-code",
      "-language:existentials",
      "-language:higherKinds",
      "-language:implicitConversions",
      "-language:reflectiveCalls",
      "-language:postfixOps",
      "-unchecked",
      "-Xlint"
    ),
    /** Set conf (xxxx.conf) in the source directory to also be included in resource */
    Compile / unmanagedResourceDirectories += baseDirectory.value / "src" / "main" / "scala",
    Test / unmanagedResourceDirectories += baseDirectory.value / "src" / "test" / "scala",
    classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat,
    scalafmtOnCompile := true
  )

  def define(name: String,
             baseDir: String,
             version: String = "",
             includeScala: Boolean = false,
             includeDeps: Boolean = false): Project =
    Project(name, file(baseDir))
      .settings(
        Projects.commonSettings ++
          Seq(
            assembly / assemblyJarName := (if (version != "") s"$name-$version.jar" else s"$name.jar"),
            assembly / assemblyOption :=
              (assembly / assemblyOption).value.copy(
                includeScala = includeScala, // set whether to include scala library
                includeDependency = includeDeps
              ),
          )
      )
}
