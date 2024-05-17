package io.klaytn.apps.worker

import io.klaytn.client.SparkRedis
import io.klaytn.utils.SlackUtil
import io.klaytn.utils.spark.{KafkaStreamingHelper}

object NFTWorkerStreaming extends KafkaStreamingHelper {
  import NFTWorkerStreamingDeps._

  def nftHolder(): Unit = {
    val redisKey = "NFTWorkerStreaming:nftHolder"
    if (SparkRedis.get(redisKey).isEmpty) {
      SparkRedis.setex(redisKey, 3600, "run")
      holderService.procNFTHolder()
      SparkRedis.del(redisKey)
    }
  }

  override def run(args: Array[String]): Unit = {
    stream().foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        val s1 = System.currentTimeMillis()
        nftHolder()
        val s2 = System.currentTimeMillis()
        if (s2 - s1 > 3000) {
          SlackUtil.sendMessage(s"NFTWorker: ${s2 - s1}")
        }
        writeOffsetAndClearCache(rdd)
      }
    }
  }
}
