package io.klaytn.utils.spark

import com.typesafe.config.ConfigValueType
import io.klaytn.utils.SlackUtil
import io.klaytn.utils.config.Cfg
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTimeZone

import java.util.TimeZone
import scala.jdk.CollectionConverters._
import scala.util.Try

trait SparkHelper {
  lazy val appName: String = this.getClass.getName.stripSuffix("$")
  lazy val jobName: String = s"$appName-${UserConfig.chainPhase}"
  lazy val jobBasePath: String = s"jobs/$appName"

  def run(args: Array[String]): Unit

  lazy implicit val spark: SparkSession = {
    val sparkConf: SparkConf = {
      val sparkConf = new SparkConf()

      sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")

      // TODO delete. not working in here. substitute by step.sh parameter
      sparkConf
        .set("spark.yarn.maxAppAttempts", "1")
        .set("spark.eventLog.rotation.enabled", "true")
        .set("spark.eventLog.rotation.interval", "600")
        .set("spark.eventLog.rotation.maxFilesToRetain", "6")
//        .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
//        .set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
//        .set("spark.hadoop.fs.s3a.committer.magic.enabled", "true")
//        .set("spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a",
//             "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory")
//        .set("fs.s3a.committer.name", "magic")
//        .set("spark.sql.sources.commitProtocolClass",
//             "org.apache.spark.sql.execution.datasources.SQLEmrOptimizedCommitProtocol")
//        .set("spark.sql.parquet.output.committer.class",
//             "com.amazon.emr.committer.EmrOptimizedSparkSqlParquetOutputCommitter")
//        .set("spark.sql.parquet.fs.optimized.committer.optimization-enabled", "true")
//        .set("spark.sql.hive.convertMetastoreParquet", "true")
//        .set("spark.sql.sources.partitionOverwriteMode", "static")
//      sparkConf.set("spark.streaming.kafka.consumer.poll.ms", "10000")

      sparkConf.set("spark.yarn.maxAppAttempts", "1")

      sparkConf.set("spark.eventLog.rotation.enabled", "true")
      sparkConf.set("spark.eventLog.rotation.interval", "600")
      sparkConf.set("spark.eventLog.rotation.maxFilesToRetain", "6")
      //      sparkConf.set("spark.streaming.kafka.consumer.poll.ms", "10000")

      if (System.getProperty("custom.config.path", "").nonEmpty) {
        val property = System.getProperty("spark.executor.extraJavaOptions", "")
        sparkConf
          .set(
            "spark.executor.extraJavaOptions",
            s"$property -Dcustom.config.path=${System.getProperty("custom.config.path")}")
      }

      Cfg.config
        .entrySet()
        .asScala
        .filter(_.getKey.startsWith("spark"))
        .foreach { x =>
          val key = x.getKey
          val value = x.getValue.valueType() match {
            case ConfigValueType.STRING  => Some(Cfg.getString(key))
            case ConfigValueType.BOOLEAN => Some(Cfg.getBoolean(key).toString)
            case ConfigValueType.NUMBER  => Some(Cfg.getNumber(key).toString)
            case _                       => None
          }
          value match {
            case Some(v) => sparkConf.set(key, v)
            case _       =>
          }
        }

      sparkConf
    }

    val phase = Cfg.config.getString("spark.app.phase")
    if (phase == "local") {
      SparkSession.builder().master("local[*]").config(sparkConf).getOrCreate()
    } else {
      SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    }
  }

  lazy val sc: SparkContext = {
    val sc = spark.sparkContext

    if (UserConfig.s3AccessKey.isSuccess && UserConfig.s3SecretKey.isSuccess) {
      sc.hadoopConfiguration
        .set("fs.s3a.access.key", UserConfig.s3AccessKey.get)
      sc.hadoopConfiguration
        .set("fs.s3a.secret.key", UserConfig.s3SecretKey.get)
    }

    DateTimeZone.setDefault(DateTimeZone.UTC)
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

    sc.setLogLevel("WARN")
    sc
  }

  def stop(): Unit = {
    spark.stop()
  }

  def setCustomConfigPath(): Unit = {
    val configFile =
      s"${appName.replaceAll("[.]", "/")}-${UserConfig.chainPhase}.conf"
    System.setProperty("custom.config.path", configFile)
  }

  def startMessage(): String = {
    s"""Start Job: $jobName
       |phase: ${UserConfig.chainPhase}
       |appId: ${sc.applicationId}
       |spark.executor.instances: ${Try(
         Cfg.getString("spark.executor.instances")).getOrElse("-")}
       |spark.executor.cores: ${Try(Cfg.getString("spark.executor.cores"))
         .getOrElse("-")}
       |""".stripMargin
  }

  def endMessage(): String = {
    s"""Finish Job: $jobName
       |phase: ${UserConfig.chainPhase}
       |appId: ${sc.applicationId}
       |""".stripMargin
  }

  def beforeJobStart(): Unit = {
    setCustomConfigPath()
    SlackUtil.sendMessage(startMessage())
  }

  def afterJobFinish(): Unit = {
    SlackUtil.sendMessage(endMessage())
  }

  def main(args: Array[String]): Unit = {
    beforeJobStart()

    run(args)

    afterJobFinish()
  }
}
