package io.klaytn.apps.worker

import io.klaytn.client.SparkRedis
import io.klaytn.utils.SlackUtil
import io.klaytn.utils.config.FunctionSupport
import io.klaytn.utils.spark.{KafkaStreamingHelper, UserConfig}
import org.apache.spark.TaskContext

object FastWorkerStreaming extends KafkaStreamingHelper {
  import FastWorkerStreamingDeps._

  def blockRestore(): Unit = {
    val redisKey = "FastWorkerStreaming:DoNotRestoreBlock"
    val lastCheckTimeRedisKey = "BlockRestore:LastCheckTime"
    // Get the last n from the blocks db.
    // Fetch the data and populate the block, eventlog, and tx if it is not already there.
    if (SparkRedis.get(redisKey).isEmpty) {
      SparkRedis.setex(redisKey, 3600, "run")

      val now = (System.currentTimeMillis() / 1000).toInt
      val lastCheckTime = SparkRedis
        .get(lastCheckTimeRedisKey)
        .getOrElse((now - 86400).toString)
        .toInt

      // Check at least 100 items even if the check time is less than 100 seconds.
      blockService.restoreMissingBlocks(Math.max(now - lastCheckTime, 100),
                                        jobBasePath)
      SparkRedis.set(lastCheckTimeRedisKey, now.toString)

      SparkRedis.del(redisKey)
    }

    val forceRestoreKey = "FastWorkerStreaming:ForceRestoreBlock"
    if (SparkRedis.get(forceRestoreKey).nonEmpty) {
      val jobKey = "FastWorkerStreaming:ForceRestoreBlockRun"
      if (SparkRedis.get(jobKey).isEmpty) {
        SparkRedis.setex(jobKey, 3600, "run")
        blockService.restoreNextBlock(jobBasePath)
        SparkRedis.del(jobKey)
      }
    }
  }

  def tokenHolder(): Unit = {
    val redisKey = "FastWorkerStreaming:tokenHolder"
    if (SparkRedis.get(redisKey).isEmpty) {
      SparkRedis.setex(redisKey, 3600, "run")
      holderService.procTokenHolder()
      SparkRedis.del(redisKey)
    }
  }

  def nftHolder(): Unit = {
    val redisKey = "FastWorkerStreaming:nftHolder"
    if (SparkRedis.get(redisKey).isEmpty) {
      SparkRedis.setex(redisKey, 3600, "run")
      holderService.procNFT(2000L)
      SparkRedis.del(redisKey)
    }
  }

  def blockBurnFee(): Unit = {
    if (!FunctionSupport.burnFee(UserConfig.chainPhase)) return

    val redisKey = "FastWorkerStreaming:blockBurnFee"
    if (SparkRedis.get(redisKey).isEmpty) {
      SparkRedis.setex(redisKey, 3600, "run")
      if (FunctionSupport.blockReward(UserConfig.chainPhase))
        blockService.procBurnFeeByBlockRewardInfo()
      else blockService.procBurnFee()
      SparkRedis.del(redisKey)
    }
  }

  def tokenBurnAmount(): Unit = {
    if (!FunctionSupport.burn(UserConfig.chainPhase)) return

    val redisKey = "FastWorkerStreaming:tokenBurnAmount"
    if (SparkRedis.get(redisKey).isEmpty) {
      SparkRedis.setex(redisKey, 3600, "run")
      holderService.procTokenBurnAmount()
      SparkRedis.del(redisKey)
    }
  }

  def nftBurnAmount(): Unit = {
    if (!FunctionSupport.burn(UserConfig.chainPhase)) return

    val redisKey = "FastWorkerStreaming:nftBurnAmount"
    if (SparkRedis.get(redisKey).isEmpty) {
      SparkRedis.setex(redisKey, 3600, "run")
      holderService.procNFTBurnAmount()
      SparkRedis.del(redisKey)
    }
  }

  override def run(args: Array[String]): Unit = {
//    val numWorkers = 3
//    val stream     = ssc.receiverStream(new WorkerMockReceiver(numWorkers * 100))
    stream().foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
//        SlackUtil.sendMessage(s"num partions: ${rdd.partitions.length} / ${rdd.repartition(3).partitions.length}")
        rdd.repartition(3).foreachPartition { x =>
//          SlackUtil.sendMessage(s"partition number: ${TaskContext.getPartitionId()} ; ${x.length}")
//          val start = System.currentTimeMillis()
          TaskContext.getPartitionId() match {
            case 0 =>
              val s1 = System.currentTimeMillis()
              blockRestore()
              // val s2 = System.currentTimeMillis()
              blockBurnFee()
              // val s3 = System.currentTimeMillis()
              tokenBurnAmount()
              // val s4 = System.currentTimeMillis()
              nftBurnAmount()
              val s5 = System.currentTimeMillis()
              if (s5 - s1 > 3000) {
                // SlackUtil.sendMessage(
                //   s"FastWorker#0: ${s5 - s1} / burn nft: ${s5 - s4} / burn token: ${s4 - s3} / block burn fee: ${s3 - s2} / restore: ${s2 - s1}")
              }
            case 1 =>
              val s1 = System.currentTimeMillis()
              tokenHolder()
              val s2 = System.currentTimeMillis()
              if (s2 - s1 > 3000) {
                // SlackUtil.sendMessage(s"FastWorker#1: ${s2 - s1}")
              }
            case 2 =>
              val s1 = System.currentTimeMillis()
              nftHolder()
              val s2 = System.currentTimeMillis()
              if (s2 - s1 > 3000) {
                SlackUtil.sendMessage(s"FastWorker#2: ${s2 - s1}")
              }
          }
//          val diff = System.currentTimeMillis() - start
//          if (diff > 3000) {
//            SlackUtil.sendMessage(
//              s"FastWorker: $diff ms; TaskContext.getPartitionId(): ${TaskContext.getPartitionId()};")
//          }
        }
      }
      writeOffsetAndClearCache(rdd)
    }
  }
}
