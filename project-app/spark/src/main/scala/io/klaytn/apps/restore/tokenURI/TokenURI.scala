package io.klaytn.apps.restore.tokenURI

import io.klaytn.client.SparkRedis
import io.klaytn.utils.SlackUtil
import io.klaytn.utils.config.FunctionSupport
import io.klaytn.utils.spark.{KafkaStreamingHelper, UserConfig}
import io.klaytn.apps.worker.WorkerMockReceiver
import org.apache.spark.TaskContext

object TokenURI extends KafkaStreamingHelper {
  import TokenURIDeps._

  def tokenURI(id: Long, limit: Long): Unit = {
    val inventories =
      holderPersistentAPI.getInventoriesByIdRange(id, id + limit)
    val inventoriesRDD = spark.sparkContext.parallelize(inventories, 100)
    inventoriesRDD.foreachPartition { partition =>
      try {
        val tokenURIs = partition
          .map { (x) =>
            val contractAddress = x._1.contractAddress
            val tokenId = x._1.tokenId
            val tokenURI =
              contractService.getFreshTokenURI(
                x._2,
                contractAddress,
                tokenId
              )
            (contractAddress, tokenId.toString, tokenURI)
          }
          .toSeq
          .filter(_._3 != "-")
        if (tokenURIs.nonEmpty)
          holderPersistentAPI.updateTokenUriBulk(tokenURIs)
      } catch {
        case e: Exception =>
          SlackUtil.sendMessage(
            s"Error in tokenURI: ${e.getMessage}"
          )
          throw e
      }
    }
  }
  override def run(args: Array[String]): Unit = {
    2967L to 16000L foreach { x =>
      val t1 = System.currentTimeMillis()
      tokenURI(x * 10000, 10000)
      SlackUtil.sendMessage(
        s"TokenURI: ${x * 10000} ~ ${x * 10000 + 10000} is done. ${System
          .currentTimeMillis() - t1} ms, ${chainPhase}"
      )
    }
  }
}
