package io.klaytn.persistent.impl.rdb

import io.klaytn.model.RefinedBlock
import io.klaytn.persistent.BlockPersistentAPI
import io.klaytn.repository.{BlockBurns, BlockRepository, BlockReward}

class RDBBlockPersistentAPI() extends BlockRepository with BlockPersistentAPI {
  override def getLatestBlockInfo(): (Long, Long) = {
    super.getLatestBlockInfo()
  }

  override def insertBlocks(blocks: List[RefinedBlock]): Unit = {
    super.insertBlocks(blocks)
  }

  override def insertBlockReward(blockReward: BlockReward): Unit = {
    super.insertBlockReward(blockReward)
  }

  override def selectRecentBlockNumbers(limit: Int): Seq[Long] = {
    super.selectRecentBlockNumbers(limit)
  }

  override def minMaxTimestampOfRecentBlock(
      limit: Int): Option[(Long, Long)] = {
    super.minMaxTimestampOfRecentBlock(limit)
  }

  override def dailyTotalTransactionCount(fromBlock: Long,
                                          toBlock: Long,
                                          fromTs: Int,
                                          toTs: Int): Long = {
    super.dailyTotalTransactionCount(fromBlock, toBlock, fromTs, toTs)
  }

  override def dailyMinMaxAccumulateBurnFees(
      fromBlock: Long,
      toBlock: Long,
      fromTs: Int,
      toTs: Int): Seq[(String, String)] = {
    super.dailyMinMaxAccumulateBurnFees(fromBlock, toBlock, fromTs, toTs)
  }

  override def totalTransactionCount(limit: Int): Long = {
    super.totalTransactionCount(limit)
  }

  override def maxBlockNumber(): Long = {
    super.maxBlockNumber()
  }

  override def getLatestBlockBurnInfo(): (Long, BigInt, BigInt) = {
    super.getLatestBlockBurnInfo()
  }

  override def insertBlockBurns(burns: Seq[BlockBurns]): Unit = {
    super.insertBlockBurns(burns)
  }

  override def findBlockFeeInfos(
      maxBlockNumber: Long): Seq[(Long, String, Int, Int)] = {
    super.findBlockFeeInfos(maxBlockNumber)
  }

  override def findBurntFeesOfBlockRewards(
      maxBlockNumber: Long): Map[Long, String] = {
    super.findBurntFeesOfBlockRewards(maxBlockNumber)
  }
}
