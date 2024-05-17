package io.klaytn.apps.worker

import io.klaytn.client.SparkRedis
import io.klaytn.utils.SlackUtil
import io.klaytn.utils.spark.{KafkaStreamingHelper}
import org.apache.spark.TaskContext

object TokenURIWorkerStreaming extends KafkaStreamingHelper {
  import TokenURIWorkerStreamingDeps._

  def tokenURI(): Unit = {
    val redisKey = "TokenURIWorkerStreaming:tokenURI"
    if (SparkRedis.get(redisKey).isEmpty) {
      SparkRedis.setex(redisKey, 3600, "run")
      holderService.procTokenURI(spark)
      SparkRedis.del(redisKey)
    }
  }

  override def run(args: Array[String]): Unit = {
    stream().foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        val s1 = System.currentTimeMillis()
        tokenURI()
        val s2 = System.currentTimeMillis()
        if (s2 - s1 > 3000) {
          SlackUtil.sendMessage(s"TokenURIWorker: ${s2 - s1}")
        }
        writeOffsetAndClearCache(rdd)
      }
    }
  }
}
