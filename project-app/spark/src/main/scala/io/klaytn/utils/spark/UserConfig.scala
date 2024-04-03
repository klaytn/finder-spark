package io.klaytn.utils.spark

import io.klaytn.model.ChainPhase
import io.klaytn.utils.config.Cfg
import org.apache.spark.streaming.{Duration => SparkDuration}

import java.time.Duration
import scala.util.Try

object UserConfig {
  lazy val chainPhase: ChainPhase = ChainPhase.get()

  lazy val logStorageS3Path: String =
    Try(Cfg.getString("spark.app.log.storage.s3.path")).getOrElse("")
  lazy val refinedLogCount: Int =
    Try(Cfg.getInt("spark.app.refined.log.count")).getOrElse(10)

  lazy val s3AccessKey: Try[String] = Try(
    Cfg.getString("spark.app.s3a.access.key"))
  lazy val s3SecretKey: Try[String] = Try(
    Cfg.getString("spark.app.s3a.secret.key"))

  lazy val projectId: Try[String] = Try(
    Cfg.getString("spark.app.gcs.project.id"))

  lazy val region: Try[String] = Try(Cfg.getString("spark.app.gcs.region"))

  lazy val baseBucket: String = Cfg.getString("spark.app.base.bucket")

  // streaming
  lazy val bootstrapServers: String =
    Cfg.getString("spark.app.kafka.bootstrap.servers")
  lazy val batchDuration: SparkDuration = SparkDuration(
    Cfg.getDuration("spark.app.batchDuration").toMillis)

  lazy val startBlockNumber: Long =
    Try(Cfg.getString("spark.app.start.block.number").toLong).getOrElse(0L)

  lazy val offsetMetaKey: String =
    Try(Cfg.getString("spark.app.offset.meta.key"))
      .getOrElse(s"kafka-offset.$chainPhase.txt")

  lazy val topicBlock: String =
    Cfg.getString("spark.app.kafka.topic.list.block")
  lazy val topicTrace: String =
    Cfg.getString("spark.app.kafka.topic.list.internalTransaction")

  lazy val consumeMode: String =
    Try(Cfg.getString("spark.app.kafka.consume.mode")).getOrElse("resume")
  lazy val useTopics: Set[String] =
    Cfg
      .getStringList("spark.app.kafka.topic.use_list")
      .map(use => Cfg.getString(s"spark.app.kafka.topic.list.$use"))
      .toSet

  // Monitoring notifications if microbatch processing time takes more than a specified amount of time
  lazy val streamingMonitoringMaxDeploy: Try[Duration] = Try(
    Cfg.getDuration("spark.app.streaming.monitoring.maxDelay"))

  // Set notification interval to avoid overwhelming monitoring results in case of storing delays
  lazy val streamingMonitoringNotifyInterval: Try[Duration] = Try(
    Cfg.getDuration("spark.app.streaming.monitoring.notifyInterval"))

  // dump kafka
  lazy val dumpKafkaNumExecutors: Int =
    Try(Cfg.getInt("spark.app.dump.kafka.num.executors")).getOrElse(4)

  lazy val numInstances: Int = Cfg.getInt("spark.executor.instances")
  lazy val numCores: Int = Cfg.getInt("spark.executor.cores")

  lazy val webHookUrl: String =
    Try(Cfg.getString("spark.app.slack.url")).getOrElse("")
}
