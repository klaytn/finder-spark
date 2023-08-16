package io.klaytn.utils.config

import io.klaytn.model.Chain
import io.klaytn.utils.spark.UserConfig

object Constants {
  val ZeroAddress = "0x0000000000000000000000000000000000000000"
  val DeadAddress = "0x000000000000000000000000000000000000dead"
  val KokoaDeadAddress = "0x0000000000000000000000000000000deadc0c0a"
  val BigIntZero: BigInt = BigInt(0)

  def isZeroOrDead(address: String): Boolean =
    address == ZeroAddress || address == DeadAddress

  def nonZeroOrDead(address: String): Boolean = !isZeroOrDead(address)

  def DefaultGasPrice(blockNumber: Long): String = {
    UserConfig.chainPhase.chain match {
      case Chain.baobab =>
        if (blockNumber <= 87091199) {
          "25000000000"
        } else if (blockNumber <= 90719999) {
          "750000000000"
        } else {
          "250000000000"
        }
      case Chain.cypress =>
        if (blockNumber <= 87091199) {
          "25000000000"
        } else if (blockNumber <= 91324799) {
          "750000000000"
        } else {
          "250000000000"
        }
    }
  }
}
