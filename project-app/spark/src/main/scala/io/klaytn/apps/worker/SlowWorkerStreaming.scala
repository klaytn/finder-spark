package io.klaytn.apps.worker

import io.klaytn.client.{FinderRedis, SparkRedis}
import io.klaytn.model.ChainPhase
import io.klaytn.utils.config.FunctionSupport
import io.klaytn.utils.spark.{KafkaStreamingHelper, UserConfig}
import io.klaytn.utils.{JsonUtil, SlackUtil}
import org.apache.spark.TaskContext

object SlowWorkerStreaming extends KafkaStreamingHelper {
  import SlowWorkerStreamingDeps._

  def stat(): Unit = {
    // Update the data below once every 120 seconds.
    val redisKey = "SlowWorkerStreaming:stat"
    if (SparkRedis.get(redisKey).isEmpty) {
      SparkRedis.setex(redisKey, 120, "run")

      // avg block time (24h)
      val avgBlockTime = blockService.getAvgBlockTime(86400)
      FinderRedis.setex("stat:AvgBlockTime:24h", 86400, avgBlockTime)

      // avg tx per block
      val avgTransactionPerBlock = blockService.getAvgTransactionPerBlock(86400)
      if (FunctionSupport.longValueAvgTX(UserConfig.chainPhase)) {
        FinderRedis.setex("stat:AvgTxPerBlock:24h",
                          86400,
                          avgTransactionPerBlock.longValue().toString)
      } else
        FinderRedis.setex("stat:AvgTxPerBlock:24h",
                          86400,
                          avgTransactionPerBlock.toString)

      // transaction history
      val transactionHistory = blockService.getTransactionHistory(30)
      FinderRedis.setex("stat:TransactionHistory:30days",
                        86400,
                        JsonUtil.asJson(transactionHistory))

      // burnt by gas fee history
      if (FunctionSupport.burnFee(UserConfig.chainPhase)) {
        val burntHistory = blockService.getBurntByGasFeeHistory(30)
        FinderRedis.setex("stat:BurntByGasFeeHistory:30days",
                          86400,
                          JsonUtil.asJson(burntHistory))
      }

//      SparkRedis.del(redisKey)
    }
  }

  def minusHolder(): Unit = {
    // Update the data below once every 120 seconds.
    val redisKey = "SlowWorkerStreaming:minusHolder"
    if (SparkRedis.get(redisKey).isEmpty) {
      SparkRedis.setex(redisKey, 600, "run")

      minusHolderService.getMinusTokenHolders().foreach {
        case (contractAddress, holderAddress) =>
          minusHolderService.modifyMinusTokenHolder(contractAddress,
                                                    holderAddress)
      }

      minusHolderService.getMinusKIP17Holders().foreach {
        case (contractAddress, holderAddress) =>
          minusHolderService.modifyMinusKIP17Holder(contractAddress,
                                                    holderAddress)
      }

      minusHolderService.getMinusKIP37Holders().foreach {
        case (contractAddress, holderAddress, tokenId) =>
          minusHolderService
            .modifyMinusKIP37Holder(contractAddress, holderAddress, tokenId)
      }
    }
  }

  def regContractFromITX(dbIndexes: Seq[Int]): Unit = {
    dbIndexes.foreach { dbIndex =>
      val redisKey = s"SlowWorkerStreaming:regContractFromITX:$dbIndex"
      if (SparkRedis.get(redisKey).isEmpty) {
        SparkRedis.setex(redisKey, 3600, "run")
        val dbName = f"finder02$dbIndex%02d"
        contractService.procRegContractFromITX(dbName + "r")
        SparkRedis.del(redisKey)
      }
    }
  }

  private def correctHolder(): Unit = {
    val redisKey = "SlowWorkerStreaming:correctHolder"
    if (SparkRedis.get(redisKey).isEmpty) {
      SparkRedis.setex(redisKey, 3600, "run")
      holderService.procCorrectTokenHolder()
      holderService.procCorrectNFTHolder()
      SparkRedis.del(redisKey)
    }
  }

  override def run(args: Array[String]): Unit = {
    import ChainPhase._

    val numWorkers = UserConfig.chainPhase match {
      case `prod-cypress` => 3
      case `prod-baobab`  => 2
      case _ =>
        throw new RuntimeException(s"wrong phase: ${UserConfig.chainPhase}")
    }

//    SlackUtil.sendMessage(s"SlowWorker #workers: $numWorkers")

//    val stream = ssc.receiverStream(new WorkerMockReceiver(numWorkers))

    stream().foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        rdd.repartition(numWorkers).foreachPartition { _ =>
          val start = System.currentTimeMillis()
          UserConfig.chainPhase match {
            case `prod-cypress` =>
              TaskContext.getPartitionId() match {
                case 0 =>
                  stat()
                  correctHolder()
                case 1 => regContractFromITX(Seq(1, 3, 5, 7, 9))
                case 2 => regContractFromITX(Seq(2, 4, 6, 8, 10))
              }
            case `prod-baobab` =>
              TaskContext.getPartitionId() match {
                case 0 =>
                  stat()
                  correctHolder()
                case 1 => regContractFromITX(Seq(1))
              }
            case _ =>
              throw new RuntimeException(
                s"wrong phase: ${UserConfig.chainPhase}")
          }
          val diff = System.currentTimeMillis() - start
          if (diff > 3000) {
            SlackUtil.sendMessage(
              s"SlowWorker: $diff ms; TaskContext.getPartitionId(): ${TaskContext.getPartitionId()}")
          }
        }
      }
      writeOffsetAndClearCache(rdd)
    }
  }
}
