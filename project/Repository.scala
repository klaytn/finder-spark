package spark_project
import sbt._

object Repository {
  val cloudera = "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
  val sonatype = "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/releases/"
  val twitter  = "Twitter Maven" at "https://maven.twttr.com"
  val jitpack  = "Jitpack" at "https://jitpack.io"
  val spring   = "spring" at "https://repo.spring.io/plugins-release/"
//  val klaytnSnapshot = ("klaytn-snapshot" at "http://nexus.klayoff.com:8081/repository/klaytn-snapshot/")
//    .withAllowInsecureProtocol(true)
//  val klaytnRelease = ("klaytn-release" at "http://nexus.klayoff.com:8081/repository/klaytn-release/")
//    .withAllowInsecureProtocol(true)
}
