package io.klaytn.utils.spark.offset

import io.klaytn.utils.JsonUtil
import io.klaytn.utils.JsonUtil.Implicits._
import io.klaytn.utils.gcs.GCSUtil
import org.apache.spark.streaming.kafka010.OffsetRange

import scala.util.Try

class GCSOffsetManager {
  def writeOffset(bucket: String,
                  key: String,
                  offsetRanges: Array[OffsetRange]): Unit = {
    val offsets = offsetRanges
      .sortBy(_.partition)
      .map { x =>
        OffsetMeta(x.topic, x.partition, x.fromOffset, x.untilOffset)
      }
    val history = OffsetHistory(System.currentTimeMillis(), offsets)
    val json = JsonUtil.asJson(history)
    GCSUtil.writeText(bucket, key, json)
  }

  def readOffset(bucket: String, key: String): Array[OffsetRange] = {
    GCSUtil.readText(bucket, key) match {
      case Some(meta) =>
        Try(JsonUtil.fromJson[OffsetHistory](meta) match {
          case Some(history) =>
            history.offsets.map { meta =>
              {
                OffsetRange(meta.topic,
                            meta.partition,
                            meta.fromOffset,
                            meta.untilOffset)
              }
            }
          case None =>
            Array.empty[OffsetRange]
        }).getOrElse(Array.empty[OffsetRange])
      case _ => Array.empty[OffsetRange]
    }
  }
}
