package io.klaytn.apps.worker

import io.klaytn.client.SparkRedis
import io.klaytn.utils.SlackUtil
import io.klaytn.utils.spark.{KafkaStreamingHelper}

object TokenWorkerStreaming extends KafkaStreamingHelper {
  import TokenWorkerStreamingDeps._

  def tokenHolder(): Unit = {
    val redisKey = "TokenWorkerStreaming:tokenHolder"
    if (SparkRedis.get(redisKey).isEmpty) {
      SparkRedis.setex(redisKey, 3600, "run")
      holderService.procTokenHolder()
      SparkRedis.del(redisKey)
    }
  }

  override def run(args: Array[String]): Unit = {
    stream().foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        val s1 = System.currentTimeMillis()
        tokenHolder()
        val s2 = System.currentTimeMillis()
        if (s2 - s1 > 3000) {
          SlackUtil.sendMessage(s"TokenWorker: ${s2 - s1}")
        }
        writeOffsetAndClearCache(rdd)
      }
    }
  }
}
