package io.klaytn.apps.restore.blockBurn

import io.klaytn.utils.spark.{KafkaStreamingHelper}

object BlockBurn extends KafkaStreamingHelper {
  import BlockBurnDeps._

  def blockBurnFee(): Unit = {
    blockService.procBurnFeeByBlockRewardInfo()
  }

  override def run(args: Array[String]): Unit = {

    140792004L to 140800000L foreach { number =>
      blockService.saveBlockRewardToMysql(number)
    }
  }
}
