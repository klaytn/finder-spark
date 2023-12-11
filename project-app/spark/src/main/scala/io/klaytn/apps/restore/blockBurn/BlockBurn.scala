package io.klaytn.apps.restore.blockBurn

import io.klaytn.client.SparkRedis
import io.klaytn.utils.SlackUtil
import io.klaytn.utils.config.FunctionSupport
import io.klaytn.utils.spark.{KafkaStreamingHelper, UserConfig}
import io.klaytn.apps.worker.WorkerMockReceiver
import org.apache.spark.TaskContext

object TokenURI extends KafkaStreamingHelper {
  import BlockBurnDeps._

  def blockBurnFee(): Unit = {
    blockService.procBurnFeeByBlockRewardInfo()
  }

  override def run(args: Array[String]): Unit = {
    0L to 139632634L foreach { x =>
      blockBurnFee()
    }
  }
}
