package io.klaytn.apps.batch.etl

import io.klaytn.client.SparkRedis
import io.klaytn.model.Block
import io.klaytn.utils.JsonUtil
import io.klaytn.utils.spark.{KafkaStreamingHelper, UserConfig}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.SaveMode

import java.nio.ByteBuffer
import java.text.SimpleDateFormat
import java.util.TimeZone
import scala.collection.mutable

/** bnp: block number partition
  * dh: date hour
  * log: log
  */
case class DumpKafkaLog2(bnp: String, dh: String, log: String)

object DumpKafkaBlockBatch extends KafkaStreamingHelper {
  import spark.implicits._

  def getDumpKafkaLog(log: String): Option[DumpKafkaLog2] = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd-HH")
    sdf.setTimeZone(TimeZone.getTimeZone("UTC"))

    Option(
      Block.parse(log) match {
        case Some(block) =>
          DumpKafkaLog2(block.blockNumberPartition,
                        sdf.format(block.timestamp.toLong * 1000),
                        log)
        case _ => null
      }
    )
  }

  def setLogToRedis(key: String,
                    value: Map[Int, String]): Option[DumpKafkaLog2] = {
    SparkRedis.setex(key, 1200, JsonUtil.asJson(value))
    None
  }

  def makeLog(topic: String,
              key: String,
              value0: Iterable[(Int, String)]): Option[DumpKafkaLog2] = {
    val s = key.split("_")
    val (blockNumber, totalSeg) = (s(0).toLong, s(1).toInt)
    val value = value0.toMap

    val redisKey = topic match {
      case UserConfig.topicBlock => s"DumpKafka:block:$blockNumber"
      case UserConfig.topicTrace => s"DumpKafka:itx:$blockNumber"
    }

    // If the current stream contains the entire log.
    if (totalSeg == value.size) {
      getDumpKafkaLog(
        (0 until totalSeg).map(idx => value.getOrElse(idx, "")).mkString)
    } else {
      SparkRedis.get(redisKey) match {
        case Some(v) =>
          JsonUtil.fromJson[mutable.Map[Int, String]](v) match {
            case Some(newValue) =>
              value.foreach { case (k, v) => newValue.put(k, v) }
              if (totalSeg == newValue.size)
                getDumpKafkaLog(
                  (0 until totalSeg)
                    .map(idx => value.getOrElse(idx, ""))
                    .mkString)
              else setLogToRedis(redisKey, newValue.toMap)
            case _ => setLogToRedis(redisKey, value)
          }
        case _ => setLogToRedis(redisKey, value)
      }
    }
  }

  def getLog(record: ConsumerRecord[String, String])
    : Option[(String, (Int, String))] = {
    val segIdx = ByteBuffer
      .wrap(record.headers().lastHeader("segmentIdx").value())
      .getLong()
      .toInt
    val totalSeg = ByteBuffer
      .wrap(record.headers().lastHeader("totalSegments").value())
      .getLong()
      .toInt
    Some((s"${record.key()}_$totalSeg", (segIdx, record.value())))
  }

  override def run(args: Array[String]): Unit = {
    val topic = topics().head
    val savePath = topic match {
      case UserConfig.topicBlock =>
        s"${UserConfig.logStorageS3Path}topic=block/"
      case UserConfig.topicTrace =>
        s"${UserConfig.logStorageS3Path}topic=trace/"
    }

    stream().foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        val data = rdd
          .flatMap(getLog)
          .groupByKey(UserConfig.dumpKafkaNumExecutors)
          .flatMap(x => makeLog(topic, x._1, x._2))

        spark
          .createDataset(data)
          .repartition(1)
          .write
          .mode(SaveMode.Append)
          .option("compression", "gzip")
          .partitionBy("bnp", "dh")
          .text(savePath)
      }

      writeOffsetAndClearCache(rdd)
    }
  }
}
