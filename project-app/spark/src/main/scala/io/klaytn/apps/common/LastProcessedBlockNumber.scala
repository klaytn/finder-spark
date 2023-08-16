package io.klaytn.apps.common

import io.klaytn.service.CaverFactory
import io.klaytn.utils.spark.UserConfig

object LastProcessedBlockNumber {
  private val MaxBefore = 43200 // to give us plenty of time to process it before about 12 hours.
  private var startBlockNumber = 0L
  private var lastProcessedBlockNumber = 0L

  private val ValidTimeOfStartBlockNumber = System
    .currentTimeMillis() + 21600000

  def getLastProcessedBlockNumber(): Long = {
    if (lastProcessedBlockNumber == 0L) {
      lastProcessedBlockNumber = BigInt(
        CaverFactory.caver.rpc.klay.getBlockNumber.send().getValue).toLong
      if (UserConfig.startBlockNumber == 0L) startBlockNumber = 1L
      else startBlockNumber = UserConfig.startBlockNumber
    }

    // If an initial block is specified, the start block number setting will be kept for 12 hours or so.
    if (System.currentTimeMillis() > ValidTimeOfStartBlockNumber)
      lastProcessedBlockNumber - MaxBefore
    else startBlockNumber
  }

  def setLastProcessedBlockNumber(blockNumber: Long): Unit = this.synchronized {
    lastProcessedBlockNumber = Math.max(blockNumber, lastProcessedBlockNumber)
  }
}
