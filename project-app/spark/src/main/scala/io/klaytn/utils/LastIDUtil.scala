package io.klaytn.utils

import io.klaytn.utils.gcs.GCSUtil
import io.klaytn.utils.spark.UserConfig

object LastIDUtil {
  private def getS3Key(lastIdS3PathPrefix: String, key: String): String =
    s"$lastIdS3PathPrefix/$key.${UserConfig.chainPhase.chain}"

  private def saveToS3(lastIdS3PathPrefix: String,
                       key: String,
                       data: String): Unit =
    GCSUtil.writeText(UserConfig.baseBucket,
                      getS3Key(lastIdS3PathPrefix, key),
                      data)

  private def getLastId(lastIdS3PathPrefix: String,
                        key: String): Option[String] =
    GCSUtil.readText(UserConfig.baseBucket, getS3Key(lastIdS3PathPrefix, key))

  private val FastWorkerLastIdS3PathPrefix =
    "jobs/io.klaytn.apps.worker.FastWorkerStreaming/lastId"
  private val SlowWorkerLastIdS3PathPrefix =
    "jobs/io.klaytn.apps.worker.SlowWorkerStreaming/lastId"

  def fastWorkerSaveToS3(key: String, data: String): Unit =
    saveToS3(FastWorkerLastIdS3PathPrefix, key, data)
  def fastWorkerGetLastId(key: String): Option[String] =
    getLastId(FastWorkerLastIdS3PathPrefix, key)

  def slowWorkerSaveToS3(key: String, data: String): Unit =
    saveToS3(SlowWorkerLastIdS3PathPrefix, key, data)
  def slowWorkerGetLastId(key: String): Option[String] =
    getLastId(SlowWorkerLastIdS3PathPrefix, key)
}
