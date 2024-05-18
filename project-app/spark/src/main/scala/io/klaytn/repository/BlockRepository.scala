package io.klaytn.repository

import io.klaytn.dsl.db.withDB
import io.klaytn.model.RefinedBlock
import io.klaytn.utils.klaytn.NumberConverter.{BigIntConverter, StringConverter}
import io.klaytn.utils.Utils
import java.text.SimpleDateFormat
import java.util.TimeZone
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object BlockRepository {
  val BlockDB: String = "finder0101"
  val BlockBurnsDB: String = "finder0101"
  val BlockRewardDB: String = "finder0101"

  val BlockTable: String = "blocks"
  val BlockBurnTable: String = "block_burns"
  val BlockRewardTable: String = "block_rewards"
}

case class BlockBurns(
    number: Long,
    fees: BigInt,
    accumulateFees: BigInt,
    accumulateKlay: BigInt,
    timestamp: Int
)
case class BlockReward(
    number: Long,
    minted: String,
    totalFee: String,
    burntFee: String,
    proposer: String,
    stakers: String,
    kgf: String,
    kir: String,
    rewards: String
)

abstract class BlockRepository extends AbstractRepository {
  import BlockRepository._

  private val df = {
    val df = new SimpleDateFormat("yyyyMM")
    df.setTimeZone(TimeZone.getTimeZone("Asia/Seoul"))
    df
  }

  def insertBlocks(blocks: List[RefinedBlock]): Unit = {
    withDB(BlockDB) { c =>
      val pstmt = c.prepareStatement(
        s"INSERT IGNORE INTO $BlockTable (`block_score`,`committee`,`extra_data`,`gas_used`,`governance_data`,`hash`," +
          "`logs_bloom`,`number`,`parent_hash`,`proposer`,`receipts_root`,`reward`,`size`,`state_root`,`timestamp`," +
          "`timestamp_fos`,`total_block_score`,`transaction_count`,`transactions_root`,`vote_data`,`date`," +
          "`base_fee_per_gas`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
      )

      val arrayBuffer = mutable.ArrayBuffer.empty[String]

      blocks.zipWithIndex.foreach {
        case (block, index) =>
          block.blockScore match {
            case Some(blockScore) => pstmt.setInt(1, blockScore)
            case _                => pstmt.setNull(1, java.sql.Types.INTEGER)
          }
//          pstmt.setString(2, JsonUtil.asJson(block.committee))
          pstmt.setString(2, null)
          pstmt.setString(3, block.extraData)
          pstmt.setInt(4, block.gasUsed)
          pstmt.setString(5, block.governanceData.orNull)
          pstmt.setString(6, block.hash)
          pstmt.setString(7, block.logsBloom)
          pstmt.setLong(8, block.number)
          pstmt.setString(9, block.parentHash)
          pstmt.setString(10, block.proposer.orNull)
          pstmt.setString(11, block.receiptsRoot)
          pstmt.setString(12, block.reward)
          pstmt.setInt(13, block.size)
          pstmt.setString(14, block.stateRoot)
          pstmt.setInt(15, block.timestamp)
          pstmt.setInt(16, block.timestampFoS)
          pstmt.setInt(17, block.totalBlockScore)
          pstmt.setInt(18, block.transactionCount)
          pstmt.setString(19, block.transactionsRoot)
          pstmt.setString(20, block.voteData)
          pstmt.setString(21, df.format(block.timestamp * 1000L))
          block.baseFeePerGas match {
            case Some(baseFeePerGas) => pstmt.setString(22, baseFeePerGas)
            case _                   => pstmt.setNull(22, java.sql.Types.VARCHAR)
          }

          pstmt.addBatch()
          pstmt.clearParameters()

          arrayBuffer.append(s"${block.number} ; ${block.hash}")

          if ((index + 1) % 3000 == 0) {
            execute(pstmt, arrayBuffer)
          }
      }

      execute(pstmt, arrayBuffer)
      pstmt.close()
    }
  }

  def insertBlockReward(blockReward: BlockReward): Unit = {
    withDB(BlockRewardDB) { c =>
      val pstmt =
        c.prepareStatement(
          s"INSERT IGNORE INTO $BlockRewardTable (`number`,`minted`,`total_fee`,`burnt_fee`,`proposer`,`stakers`,`kgf`," +
            "`kir`,`rewards`) VALUES (?,?,?,?,?,?,?,?,?)"
        )

      pstmt.setLong(1, blockReward.number)
      pstmt.setString(2, blockReward.minted)
      pstmt.setString(3, blockReward.totalFee)
      pstmt.setString(4, blockReward.burntFee)
      pstmt.setString(5, blockReward.proposer)
      pstmt.setString(6, blockReward.stakers)
      pstmt.setString(7, blockReward.kgf)
      pstmt.setString(8, blockReward.kir)
      pstmt.setString(9, blockReward.rewards)

      pstmt.execute()
      pstmt.close()
    }
  }

  def selectRecentBlockNumbers(limit: Int): Seq[Long] = {
    withDB(BlockDB) { c =>
      val pstmt = c.prepareStatement(
        s"SELECT number FROM $BlockTable ORDER BY number DESC LIMIT $limit"
      )
      val rs = pstmt.executeQuery()

      val result = ArrayBuffer.empty[Long]
      while (rs.next()) {
        result.append(rs.getLong(1))
      }

      rs.close()
      pstmt.close()

      result
    }
  }

  def minMaxTimestampOfRecentBlock(limit: Int): Option[(Long, Long)] = {
    withDB(BlockDB) { c =>
      val pstmt = c.prepareStatement(
        "SELECT MIN(timestamp), MAX(timestamp) FROM (" +
          s"SELECT timestamp FROM $BlockTable ORDER BY number DESC LIMIT $limit) a"
      )

      val rs = pstmt.executeQuery()
      val result = if (rs.next()) {
        Some((rs.getLong(1), rs.getLong(2)))
      } else {
        None
      }

      rs.close()
      pstmt.close()

      result
    }
  }

  def dailyTotalTransactionCount(
      fromBlock: Long,
      toBlock: Long,
      fromTs: Int,
      toTs: Int
  ): Long = {
    withDB(BlockDB) { c =>
      val pstmt = c.prepareStatement(
        s"SELECT SUM(transaction_count) FROM $BlockTable WHERE number >= $fromBlock AND number < $toBlock" +
          s" AND timestamp >= $fromTs AND timestamp < $toTs"
      )

      val rs = pstmt.executeQuery()
      val result = if (rs.next()) {
        rs.getLong(1)
      } else {
        0L
      }

      rs.close()
      pstmt.close()

      result
    }
  }

  def dailyMinMaxAccumulateBurnFees(
      fromBlock: Long,
      toBlock: Long,
      fromTs: Int,
      toTs: Int
  ): Seq[(String, String)] = {
    val (minBlockNumber, maxBlockNumber) = withDB(BlockBurnsDB) { c =>
      val pstmt = c.prepareStatement(
        s"SELECT MIN(number), MAX(number) FROM $BlockBurnTable WHERE number >= $fromBlock AND number < $toBlock" +
          s" AND timestamp >= $fromTs AND timestamp < $toTs"
      )

      val rs = pstmt.executeQuery()
      val result = if (rs.next()) {
        (rs.getLong(1), rs.getLong(2))
      } else {
        (0L, 0L)
      }

      rs.close()
      pstmt.close()

      result
    }

    val result = ArrayBuffer.empty[(String, String)]
    withDB(BlockBurnsDB) { c =>
      val pstmt = c.prepareStatement(
        s"SELECT accumulate_fees, accumulate_klay FROM $BlockBurnTable WHERE number IN (?, ?)"
      )
      pstmt.setLong(1, minBlockNumber)
      pstmt.setLong(2, maxBlockNumber)

      val rs = pstmt.executeQuery()
      while (rs.next()) {
        result.append((rs.getString(1), rs.getString(2)))
      }

      rs.close()
      pstmt.close()
    }
    result
  }

  def totalTransactionCount(limit: Int): Long = {
    withDB(BlockDB) { c =>
      val pstmt = c.prepareStatement(
        "SELECT SUM(transaction_count) FROM (" +
          s"SELECT transaction_count FROM $BlockTable ORDER BY number DESC LIMIT $limit) a"
      )

      val rs = pstmt.executeQuery()
      val result = if (rs.next()) {
        rs.getLong(1)
      } else {
        0L
      }

      rs.close()
      pstmt.close()

      result
    }
  }

  def maxBlockNumber(): Long = {
    withDB(BlockDB) { c =>
      val pstmt = c.prepareStatement(
        s"SELECT number FROM $BlockTable ORDER BY number DESC LIMIT 1"
      )

      val rs = pstmt.executeQuery()
      val result = if (rs.next()) {
        rs.getLong(1)
      } else {
        0L
      }

      rs.close()
      pstmt.close()

      result
    }
  }

  def getLatestBlockInfo(): (Long, Long) = {
    withDB(BlockDB) { c =>
      val pstmt = c.prepareStatement(
        s"SELECT `number`, `timestamp` FROM $BlockTable ORDER BY number DESC LIMIT 1"
      )
      val rs = pstmt.executeQuery()
      val result = if (rs.next()) {
        (rs.getLong(1), rs.getLong(2) * 1000)
      } else {
        (0L, 0L)
      }
      rs.close()
      pstmt.close()

      result
    }
  }

  def getLatestBlockBurnInfo(): (Long, BigInt, BigInt) = {
    withDB(BlockBurnsDB) { c =>
      val pstmt =
        c.prepareStatement(
          s"SELECT `number`,`accumulate_fees`,`accumulate_klay` FROM $BlockBurnTable ORDER BY `number` DESC LIMIT 1"
        )
      val rs = pstmt.executeQuery()
      val (blockNumber, accumulateFees, accumulateKlay) =
        if (rs.next())
          (
            rs.getLong(1),
            rs.getString(2).hexToBigInt(),
            rs.getString(3).hexToBigInt()
          )
        else (0L, BigInt(0), BigInt(0))

      rs.close()
      pstmt.close()
      (blockNumber, accumulateFees, accumulateKlay)
    }
  }

  def insertBlockBurns(burns: Seq[BlockBurns]): Unit = {
    withDB(BlockBurnsDB) { c =>
      val pstmt = c.prepareStatement(
        s"INSERT IGNORE  INTO $BlockBurnTable (`number`,`fees`,`accumulate_fees`,`accumulate_klay`,`timestamp`) VALUES (?,?,?,?,?)"
      )

      burns.sortBy(_.number).foreach { burn =>
        pstmt.setLong(1, burn.number)
        pstmt.setString(2, burn.fees.to64BitsHex())
        pstmt.setString(3, burn.accumulateFees.to64BitsHex())
        pstmt.setString(4, burn.accumulateKlay.to64BitsHex())
        pstmt.setInt(5, burn.timestamp)
        pstmt.addBatch()
        pstmt.clearParameters()
      }

      pstmt.executeBatch()
      pstmt.clearBatch()
      pstmt.close()
    }
  }

  def findBlockFeeInfos(maxBlockNumber: Long): Seq[(Long, String, Int, Int)] = {
    val blockInfos = mutable.ArrayBuffer.empty[(Long, String, Int, Int)]

    withDB(BlockDB) { c =>
      val pstmt =
        c.prepareStatement(
          s"SELECT `number`,`base_fee_per_gas`,`gas_used`,`timestamp` FROM $BlockTable WHERE `number` > ? ORDER BY `number` LIMIT 1000"
        )
      pstmt.setLong(1, maxBlockNumber)
      val rs = pstmt.executeQuery()
      while (rs.next()) {
        blockInfos.append(
          (rs.getLong(1), rs.getString(2), rs.getInt(3), rs.getInt(4))
        )
      }
      rs.close()
      pstmt.close()
    }

    blockInfos
  }

  def findBurntFeesOfBlockRewards(maxBlockNumber: Long): Map[Long, String] = {
    val burntFees = mutable.ArrayBuffer.empty[(Long, String)]
    withDB(BlockRewardDB) { c =>
      val pstmt = c.prepareStatement(
        s"SELECT `number`,`burnt_fee` FROM $BlockRewardTable WHERE `number` > ? ORDER BY `number` LIMIT 1000"
      )
      pstmt.setLong(1, maxBlockNumber)
      val rs = pstmt.executeQuery()
      while (rs.next()) {
        burntFees.append((rs.getLong(1), rs.getString(2)))
      }
      rs.close()
      pstmt.close()
    }
    burntFees.toMap
  }

  def getBlocksByRange(from: Long, to: Long): Seq[RefinedBlock] = {
    withDB(BlockDB) { c =>
      val pstmt = c.prepareStatement(
        s"SELECT * FROM $BlockTable b WHERE b.`number` BETWEEN ? and ?"
      )
      pstmt.setLong(1, from)
      pstmt.setLong(2, to)
      val rs = pstmt.executeQuery()

      val blocks = ArrayBuffer.empty[RefinedBlock]
      while (rs.next()) {
        val block = RefinedBlock(
          "blocks",
          Utils.getBlockNumberPartition(rs.getLong("number")),
          Option(rs.getString("base_fee_per_gas")),
          Option(rs.getInt("block_score")),
          Option(rs.getString("committee")).getOrElse("[]").split(",").toList,
          rs.getString("extra_data"),
          rs.getInt("gas_used"),
          Option(rs.getString("governance_data")),
          rs.getString("hash"),
          rs.getString("logs_bloom"),
          rs.getLong("number"),
          rs.getString("parent_hash"),
          Option(rs.getString("proposer")),
          rs.getString("receipts_root"),
          rs.getString("reward"),
          rs.getInt("size"),
          rs.getString("state_root"),
          rs.getInt("timestamp"),
          rs.getInt("timestamp_fos"),
          rs.getInt("total_block_score"),
          rs.getString("transactions_root"),
          rs.getString("vote_data"),
          rs.getInt("transaction_count")
        )

        blocks.append(block)
      }
      rs.close()
      pstmt.close()

      blocks
    }
  }

}
