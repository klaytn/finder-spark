package io.klaytn.persistent

import io.klaytn.model.{ChainPhase, RefinedBlock}
import io.klaytn.persistent.impl.rdb.RDBBlockPersistentAPI
import io.klaytn.repository.{BlockBurns, BlockReward}

object BlockPersistentAPI {
  def of(chainPhase: ChainPhase): BlockPersistentAPI = {
    chainPhase.chain match {
      case _ => new RDBBlockPersistentAPI()
    }
  }
}

trait BlockPersistentAPI extends Serializable {
  def getLatestBlockInfo(): (Long, Long)
  def insertBlocks(blocks: List[RefinedBlock]): Unit
  def insertBlockReward(blockReward: BlockReward): Unit
  def selectRecentBlockNumbers(limit: Int): Seq[Long]
  def minMaxTimestampOfRecentBlock(limit: Int): Option[(Long, Long)]
  def dailyTotalTransactionCount(fromBlock: Long,
                                 toBlock: Long,
                                 fromTs: Int,
                                 toTs: Int): Long
  def dailyMinMaxAccumulateBurnFees(fromBlock: Long,
                                    toBlock: Long,
                                    fromTs: Int,
                                    toTs: Int): Seq[(String, String)]
  def totalTransactionCount(limit: Int): Long
  def maxBlockNumber(): Long
  def getLatestBlockBurnInfo(): (Long, BigInt, BigInt)
  def insertBlockBurns(burns: Seq[BlockBurns]): Unit
  def findBlockFeeInfos(maxBlockNumber: Long): Seq[(Long, String, Int, Int)]
  def findBurntFeesOfBlockRewards(maxBlockNumber: Long): Map[Long, String]
}
