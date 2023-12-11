package io.klaytn.utils.spark.offset

import io.klaytn.utils.JsonUtil
import io.klaytn.utils.JsonUtil.Implicits._
import io.klaytn.client.SparkRedis
import org.apache.spark.streaming.kafka010.OffsetRange

import scala.util.Try

class RedisOffsetManager {
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
    val redisKey = s"${bucket}:${key}"
    SparkRedis.set(redisKey, json)
  }

  def readOffset(bucket: String, key: String): Array[OffsetRange] = {
    val redisKey = s"${bucket}:${key}"
    val json = SparkRedis.get(redisKey)
    json match {
      case Some(meta) =>
        Try(JsonUtil.fromJson[OffsetHistory](meta) match {
          case Some(history) =>
            history.offsets.map { meta =>
              OffsetRange(meta.topic,
                          meta.partition,
                          meta.fromOffset,
                          meta.untilOffset)
            }
          case None =>
            Array.empty[OffsetRange]
        }).getOrElse(Array.empty[OffsetRange])
      case _ => Array.empty[OffsetRange]
    }
  }
}
