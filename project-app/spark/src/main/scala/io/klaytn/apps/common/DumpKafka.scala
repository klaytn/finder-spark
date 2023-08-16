package io.klaytn.apps.common

import io.klaytn.client.SparkRedis
import io.klaytn.model.DumpKafkaLog
import io.klaytn.utils.spark.{KafkaStreamingHelper, UserConfig}
import io.klaytn.utils.{JsonUtil, Utils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
import org.apache.spark.storage.StorageLevel

import java.nio.ByteBuffer
import scala.collection.mutable

abstract class DumpKafka extends KafkaStreamingHelper {
  import spark.implicits._

  def postProcess(dumpKafkaLog: RDD[DumpKafkaLog]): Unit

  override def run(args: Array[String]): Unit = {
    val topic = topics().head
    val savePath = topic match {
      case UserConfig.topicBlock =>
        s"${UserConfig.logStorageS3Path}label=kafka_log/topic=block/"
      case UserConfig.topicTrace =>
        s"${UserConfig.logStorageS3Path}label=kafka_log/topic=trace/"
    }

    stream().foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        val data = rdd
          .flatMap { r =>
            if (r.key().toLong >= LastProcessedBlockNumber
                  .getLastProcessedBlockNumber()) {
              val segIdx = ByteBuffer
                .wrap(r.headers().lastHeader("segmentIdx").value())
                .getLong()
                .toInt
              val totalSeg = ByteBuffer
                .wrap(r.headers().lastHeader("totalSegments").value())
                .getLong()
                .toInt
              LastProcessedBlockNumber.setLastProcessedBlockNumber(
                r.key().toLong)
              Some((s"${r.key()}_$totalSeg", (segIdx, r.value())))
            } else {
              None
            }
          }
          .groupByKey(UserConfig.dumpKafkaNumExecutors)
          .flatMap {
            case (k, v) =>
              val s = k.split("_")
              val (blockNumber, totalSeg, value) =
                (s(0).toLong, s(1).toInt, v.toMap)

              val redisKey = topic match {
                case UserConfig.topicBlock => s"DumpKafka:block:$blockNumber"
                case UserConfig.topicTrace => s"DumpKafka:itx:$blockNumber"
              }

              // If the current stream contains the entire log.
              if (totalSeg == value.size) {
                Some(
                  DumpKafkaLog(Utils.getBlockNumberPartition(blockNumber),
                               (0 until totalSeg)
                                 .map(idx => value.getOrElse(idx, ""))
                                 .mkString))
              } else {
                SparkRedis.get(redisKey) match {
                  case Some(v) =>
                    JsonUtil.fromJson[mutable.Map[Int, String]](v) match {
                      case Some(newValue) =>
                        value.foreach { case (k, v) => newValue.put(k, v) }
                        if (totalSeg == newValue.size) {
                          Some(
                            DumpKafkaLog(
                              Utils.getBlockNumberPartition(blockNumber),
                              (0 until totalSeg)
                                .map(idx => newValue.getOrElse(idx, ""))
                                .mkString))
                        } else {
                          SparkRedis.setex(redisKey,
                                           1200,
                                           JsonUtil.asJson(newValue))
                          None
                        }
                      case _ =>
                        SparkRedis.setex(redisKey, 1200, JsonUtil.asJson(value))
                        None
                    }
                  case _ =>
                    SparkRedis.setex(redisKey, 1200, JsonUtil.asJson(value))
                    None
                }
              }
          }

        data.persist(StorageLevel.MEMORY_ONLY)

        spark
          .createDataset(data)
          .repartition(1)
          .write
          .mode(SaveMode.Append)
          .option("compression", "gzip")
          .partitionBy("bnp")
          .text(savePath)

        postProcess(data)
      }

      writeOffsetAndClearCache(rdd)
    }
  }
}
