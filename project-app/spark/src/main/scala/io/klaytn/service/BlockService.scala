package io.klaytn.service

import io.klaytn._
import io.klaytn.client.FinderRedis
import io.klaytn.model.finder.TokenTransfer
import io.klaytn.model.{
  Block,
  RefinedBlock,
  RefinedEventLog,
  RefinedTransactionReceipt
}
import io.klaytn.persistent.{
  BlockPersistentAPI,
  EventLogPersistentAPI,
  TransactionPersistentAPI
}
import io.klaytn.repository.{BlockBurns, BlockReward}
import io.klaytn.utils.config.FunctionSupport
import io.klaytn.utils.klaytn.NumberConverter.{BigIntConverter, StringConverter}
import io.klaytn.utils.spark.UserConfig
import io.klaytn.utils.{JsonUtil, SlackUtil, Utils}
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.mapAsScalaMapConverter

case class NFTTransferForDS(contractAddress: String,
                            from: String,
                            to: String,
                            tokenCount: String,
                            tokenId: String,
                            timestamp: Int,
                            blockNumber: Long,
                            transactionHash: String,
                            displayOrder: String)

class BlockService(blockPersistentAPI: LazyEval[BlockPersistentAPI],
                   transactionPersistentAPI: LazyEval[TransactionPersistentAPI],
                   eventLogPersistentAPI: LazyEval[EventLogPersistentAPI],
                   caverService: LazyEval[CaverService],
                   loadDataInfileService: LazyEval[LoadDataInfileService])
    extends Serializable {

  private def restoreBlock(blockNumber: Long, jobBasePath: String): Unit = {
    try {
      val block = caverService.getBlock(blockNumber)
      val (_, loadFile) = process(block, jobBasePath)
      loadFile.foreach {
        case (k, v) =>
          if (k != "blockNumber") {
            loadDataInfileService.loadDataAndDeleteFile(v, None)
          }
      }
      SlackUtil.sendMessage(s"restore block: $blockNumber")
    } catch {
      case e: Throwable =>
        SlackUtil.sendMessage(s"""error restore block: $blockNumber
                                 |${e.getMessage}
                                 |${StringUtils.abbreviate(
                                   ExceptionUtils.getStackTrace(e),
                                   500)}
                                 |""".stripMargin)
    }
  }

  // restore blocks after the current division's final block value
  def restoreNextBlock(jobBasePath: String): Unit = {
    val (blockNumber, timestamp) = blockPersistentAPI.getLatestBlockInfo()
    // If the current time differs by one second
    if (timestamp < System.currentTimeMillis() - 1000) {
      val totalRestoreCount = (System.currentTimeMillis() - timestamp) / 1000
      1 to Math.min(totalRestoreCount.toInt, 20) foreach { cnt =>
        try {
          restoreBlock(blockNumber + cnt, jobBasePath)
        } catch {
          case _: Throwable =>
        }
      }
    }
  }

  def restoreMissingBlocks(checkRecentCount: Int, jobBasePath: String): Unit = {
    // Remove and check the last n seconds as they may be out of order.
    val n = 5
    val blockNumbers = blockPersistentAPI
      .selectRecentBlockNumbers(checkRecentCount)
      .sorted
      .take(checkRecentCount - n)
    if (blockNumbers.last - blockNumbers.head != checkRecentCount - (n + 1)) {
      val all = (blockNumbers.head to blockNumbers.last).toSet
      val needs = all -- blockNumbers
      needs.foreach(blockNumber => restoreBlock(blockNumber, jobBasePath))
    }
  }

  def getAvgBlockTime(seconds: Int): String = {
    blockPersistentAPI.minMaxTimestampOfRecentBlock(seconds) match {
      case Some((min, max)) =>
        f"${(max - min) / 86400f}%.1f"
      case _ => "0.0"
    }
  }

  def getLastBlockTimestamp: Long = {
    blockPersistentAPI.minMaxTimestampOfRecentBlock(5) match {
      case Some((_, last)) => last
      case _               => 0
    }
  }

  def getAvgTransactionPerBlock(seconds: Int): Double = {
    val v = 100000.0 // Only up to 5 decimal places.
    Math.round(
      blockPersistentAPI.totalTransactionCount(seconds) / seconds
        .doubleValue() * v) / v
  }

  private def getHistoryData(days: Int,
                             redisKeyPrefix: String): Map[String, BigInt] = {
    val df = new SimpleDateFormat("yyyy-MM-dd")

    val cal = Calendar.getInstance()
    cal.set(Calendar.HOUR_OF_DAY, 0)
    cal.set(Calendar.MINUTE, 0)
    cal.set(Calendar.SECOND, 0)
    cal.add(Calendar.DATE, 1)

    val tsList = Seq(cal.getTimeInMillis / 1000) ++ (1 to days).map { dayAgo =>
      cal.add(Calendar.DATE, -1)
      cal.getTimeInMillis / 1000
    }

    val currentBlockNumber = blockPersistentAPI.maxBlockNumber()

    tsList
      .sliding(2, 1)
      .zipWithIndex
      .map {
        case (ts, index) =>
          val (to, from) = (ts.head.toInt, ts.last.toInt)

          val date = df.format(from * 1000L)
          val redisKey = s"$redisKeyPrefix:$date"

          val countFromRedis = FinderRedis.get(redisKey)

          // Fetch data from the DB if redis has no values or it's today.
          val count = if (index == 0 || countFromRedis.isEmpty) {
            val cnt =
              if (redisKeyPrefix == "DailyTransactionCount") {
                val result = blockPersistentAPI.dailyTotalTransactionCount(
                  currentBlockNumber - (index + 1) * 86400,
                  currentBlockNumber - (index - 1) * 86400,
                  from,
                  to)
                BigInt(result)
              } else {
                val result = blockPersistentAPI
                  .dailyMinMaxAccumulateBurnFees(
                    currentBlockNumber - (index + 1) * 86400,
                    currentBlockNumber - (index - 1) * 86400,
                    from,
                    to)
                  .map(_.hexToBigInt())
                  .sorted

                result.last - result.head
              }

            // TTL: 40 days
            if (index != 0) { // cache only when not today
              FinderRedis.setex(redisKey, 3456000, cnt.toString)
            }
            cnt
          } else {
            BigInt(countFromRedis.get)
          }

          (date, count)
      }
      .toMap
  }

  def getTransactionHistory(days: Int): Map[String, BigInt] = {
    getHistoryData(days, "DailyTransactionCount")
  }

  def getBurntByGasFeeHistory(days: Int): Map[String, BigInt] = {
    getHistoryData(days, "DailyBurntByGasFee")
  }

  def saveBlockToMysql(blocks: List[RefinedBlock]): Unit = {
    try {
      blockPersistentAPI.insertBlocks(blocks)
    } catch {
      case _: Throwable =>
    }
  }

  def saveTransactionReceiptsToMysql(
      transactionReceipts: List[RefinedTransactionReceipt]): Unit = {
    try {
      transactionPersistentAPI.insertTransactionReceipts(transactionReceipts)
    } catch {
      case _: Throwable =>
        transactionReceipts.foreach { r =>
          try {
            transactionPersistentAPI.insertTransactionReceipts(List(r))
          } catch {
            case _: Throwable =>
          }
        }
    }
  }

  def saveEventLogToMysql(eventLogs: Seq[RefinedEventLog]): Unit = {
    try {
      eventLogPersistentAPI.insertEventLogs(eventLogs)
    } catch {
      case _: Throwable =>
        eventLogs.foreach { r =>
          try {
            eventLogPersistentAPI.insertEventLogs(List(r))
          } catch {
            case _: Throwable =>
          }
        }
    }
  }

  private def saveAsBNPPartition(
      blockDS: Dataset[RefinedBlock],
      transactionReceiptDS: Dataset[RefinedTransactionReceipt],
      eventLogDS: Dataset[RefinedEventLog],
      saveDir: String,
      numPartitions: Int): Unit = {
    blockDS
      .drop("label")
      .repartition(numPartitions)
      .write
      .mode(SaveMode.Append)
      .partitionBy("bnp")
      .parquet(saveDir + "label=blocks/")

    transactionReceiptDS
      .drop("label", "tokenTransferCount", "nftTransferCount")
      .repartition(numPartitions)
      .write
      .mode(SaveMode.Append)
      .partitionBy("bnp")
      .parquet(saveDir + "label=transaction_receipts/")

    eventLogDS
      .drop("label")
      .repartition(numPartitions)
      .write
      .mode(SaveMode.Append)
      .partitionBy("bnp")
      .parquet(saveDir + "label=event_logs/")
  }

  private val sdf = new SimpleDateFormat("yyyy-MM-dd")
  sdf.setTimeZone(TimeZone.getTimeZone("UTC"))
  private val udfDate: UserDefinedFunction = udf { (ts: Int, typ: String) =>
    val dt = sdf.format(ts.toLong * 1000L)
    typ match {
      case "year"  => dt.split("-")(0)
      case "month" => dt.split("-")(1)
      case "day"   => dt.split("-")(2)
      case _       => ""
    }
  }

  private def saveAsDatePartition(
      blockDS: Dataset[RefinedBlock],
      transactionReceiptDS: Dataset[RefinedTransactionReceipt],
      eventLogDS: Dataset[RefinedEventLog],
      saveDir: String,
      numPartitions: Int): Unit = {
    blockDS
      .drop("label")
      .withColumn("year", udfDate(col("timestamp"), lit("year")))
      .withColumn("month", udfDate(col("timestamp"), lit("month")))
      .withColumn("day", udfDate(col("timestamp"), lit("day")))
      .repartition(numPartitions)
      .write
      .mode(SaveMode.Append)
      .partitionBy("year", "month", "day")
      .parquet(saveDir + "label=blocks_dt/")

    transactionReceiptDS
      .drop("label", "tokenTransferCount", "nftTransferCount")
      .withColumn("year", udfDate(col("timestamp"), lit("year")))
      .withColumn("month", udfDate(col("timestamp"), lit("month")))
      .withColumn("day", udfDate(col("timestamp"), lit("day")))
      .repartition(numPartitions)
      .write
      .mode(SaveMode.Append)
      .partitionBy("year", "month", "day")
      .parquet(saveDir + "label=transaction_receipts_dt/")

    eventLogDS
      .drop("label")
      .repartition(numPartitions)
      .withColumn("year", udfDate(col("timestamp"), lit("year")))
      .withColumn("month", udfDate(col("timestamp"), lit("month")))
      .withColumn("day", udfDate(col("timestamp"), lit("day")))
      .write
      .mode(SaveMode.Append)
      .partitionBy("year", "month", "day")
      .parquet(saveDir + "label=event_logs_dt/")
  }

  def saveTransfers(eventLogs: RDD[RefinedEventLog],
                    saveDir: String,
                    numPartitions: Int)(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    val tokenTransferEventLogs =
      eventLogs.filter(log => log.isTokenTransferEvent).map { log =>
        val (address, from, to, _, _) = log.extractTransferInfo()
        TokenTransfer(
          0L,
          address,
          from,
          to,
          log.data,
          log.timestamp,
          log.blockNumber,
          log.transactionHash,
          Utils.getDisplayOrder(log.blockNumber,
                                log.transactionIndex,
                                log.logIndex)
        )
      }

    val nftTransferEventLogs =
      eventLogs.filter(log => log.isNFTTransferEvent).flatMap { log =>
        val (address, from, to, tokenIds, values) = log.extractTransferInfo()
        tokenIds.zipWithIndex.map {
          case (tokenId, idx) =>
            NFTTransferForDS(
              address,
              from,
              to,
              values(idx),
              tokenId.toString(),
              log.timestamp,
              log.blockNumber,
              log.transactionHash,
              Utils.getDisplayOrder(log.blockNumber,
                                    log.transactionIndex,
                                    log.logIndex)
            )
        }
      }

    val udfHexToBigInt: UserDefinedFunction = udf { (hex: String) =>
      hex.hexToBigInt().toString
    }

    spark
      .createDataset(tokenTransferEventLogs)
      .drop("id")
      .repartition(numPartitions)
      .withColumn("amount_bigint", udfHexToBigInt(col("amount")))
      .withColumn("year", udfDate(col("timestamp"), lit("year")))
      .withColumn("month", udfDate(col("timestamp"), lit("month")))
      .withColumn("day", udfDate(col("timestamp"), lit("day")))
      .write
      .mode(SaveMode.Append)
      .partitionBy("year", "month", "day")
      .parquet(saveDir + "label=token_transfers_dt/")

    spark
      .createDataset(nftTransferEventLogs)
      .repartition(numPartitions)
      .withColumn("tokenCount_bigint", udfHexToBigInt(col("tokenCount")))
      .withColumn("year", udfDate(col("timestamp"), lit("year")))
      .withColumn("month", udfDate(col("timestamp"), lit("month")))
      .withColumn("day", udfDate(col("timestamp"), lit("day")))
      .write
      .mode(SaveMode.Append)
      .partitionBy("year", "month", "day")
      .parquet(saveDir + "label=nft_transfers_dt/")
  }

  def saveBlockToS3(
      data: RDD[
        (RefinedBlock, List[RefinedTransactionReceipt], List[RefinedEventLog])],
      saveDir: String,
      numPartitions: Int,
      includeByBnp: Boolean)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    val refinedBlocks = data.map(_._1)
    val refinedTransactionReceipts = data.flatMap(_._2)
    val refinedEventLogs = data.flatMap(_._3)

    val blockDS = spark.createDataset(refinedBlocks)
    val transactionReceiptDS = spark.createDataset(refinedTransactionReceipts)
    val eventLogDS = spark.createDataset(refinedEventLogs)

    // partitioned by bnp
    if (includeByBnp)
      saveAsBNPPartition(blockDS,
                         transactionReceiptDS,
                         eventLogDS,
                         saveDir,
                         numPartitions)

    // partitioned by date
    saveAsDatePartition(blockDS,
                        transactionReceiptDS,
                        eventLogDS,
                        saveDir,
                        numPartitions)

    // transfers
    saveTransfers(refinedEventLogs, saveDir, numPartitions)
  }

  def saveBlockRewardToMysql(blockNumber: Long): Unit = {
    val result = caverService.getCaver.rpc.klay
      .getRewards(BigInt(blockNumber).bigInteger)
      .send()
      .getResult

    blockPersistentAPI.insertBlockReward(
      BlockReward(
        blockNumber,
        BigInt(result.getMinted).to64BitsHex(),
        BigInt(result.getTotalFee).to64BitsHex(),
        BigInt(result.getBurntFee).to64BitsHex(),
        BigInt(result.getProposer).to64BitsHex(),
        BigInt(result.getStakers).to64BitsHex(),
        BigInt(result.getKff).to64BitsHex(),
        BigInt(result.getKcf).to64BitsHex(),
        JsonUtil.asJson(
          result.getRewards.asScala
            .map(x => (x._1, BigInt(x._2).to64BitsHex()))
            .toMap)
      )
    )
  }

  def process(block: Block,
              jobBasePath: String): (Seq[String], Seq[(String, String)]) = {
    val longTime = ArrayBuffer.empty[String]
    var start = System.currentTimeMillis()

    val refinedData = block.toRefined

    longTime.append(
      s"#2 block.toRefined: ${System.currentTimeMillis() - start}")
    start = System.currentTimeMillis()

    val refinedBlock = refinedData._1
    val refinedTransactionReceipts = refinedData._2
    val refinedEventLogs = refinedData._3

    val result = ArrayBuffer.empty[(String, String)]
    result.append(("blockNumber", refinedBlock.number.toString))

    this.saveBlockToMysql(List(refinedBlock))
    if (FunctionSupport.blockReward(UserConfig.chainPhase))
      saveBlockRewardToMysql(refinedBlock.number)

    longTime.append(s"#2 insert block: ${System.currentTimeMillis() - start}")
    start = System.currentTimeMillis()

    if (refinedTransactionReceipts.length < 300) {
      this.saveTransactionReceiptsToMysql(refinedTransactionReceipts)
      longTime.append(
        s"#2 insert tx: ${System.currentTimeMillis() - start} ; size: ${refinedTransactionReceipts.length}")
    } else {
      val transactionLines =
        loadDataInfileService.transactionReceiptLines(
          refinedTransactionReceipts)
      val txLoadFile = loadDataInfileService.writeLoadData(jobBasePath,
                                                           "tx",
                                                           refinedBlock.number,
                                                           transactionLines)
      if (txLoadFile.isDefined) {
        result.append(("tx", txLoadFile.get))
      }
      longTime.append(
        s"#2 write load file tx: ${System.currentTimeMillis() - start} ; size: ${refinedTransactionReceipts.length}")
    }
    start = System.currentTimeMillis()

    if (refinedEventLogs.length < 300) {
      this.saveEventLogToMysql(refinedEventLogs)
      longTime.append(
        s"#3 insert event log: ${System.currentTimeMillis() - start} ; size: ${refinedEventLogs.length}")
    } else {
      val eventLogLines = loadDataInfileService.eventLogLines(refinedEventLogs)
      val logLoadFile = loadDataInfileService.writeLoadData(jobBasePath,
                                                            "eventlog",
                                                            refinedBlock.number,
                                                            eventLogLines)
      if (logLoadFile.isDefined) {
        result.append(("eventLog", logLoadFile.get))
      }
      longTime.append(s"#3 write load file event log: ${System
        .currentTimeMillis() - start} ; size: ${refinedEventLogs.length}")
    }
    start = System.currentTimeMillis()

    FinderRedis.setex(s"block:${refinedBlock.number}",
                      3600,
                      refinedBlock.timestamp.toString)
    longTime.append(
      s"#4 set block to redis: ${System.currentTimeMillis() - start}")

    (longTime, result)
  }

  def procBurnFeeByBlockRewardInfo(): Unit = {
    val (maxBlockNumber, accumulateFees0, accumulateKlay) =
      blockPersistentAPI.getLatestBlockBurnInfo()

    var expectedBlockNumber = maxBlockNumber + 1 // process data only as far as the block is sequentially incremented

    val blockInfos = mutable.ArrayBuffer.empty[(Long, String, Int, Int)]

    val blocks =
      blockPersistentAPI.findBlockFeeInfos(maxBlockNumber).sortBy(_._1)
    val burntFeesMap =
      blockPersistentAPI.findBurntFeesOfBlockRewards(maxBlockNumber)

    blocks.foreach {
      case (blockNumber, baseFeePerGas, gasUsed, timestamp) =>
        if (blockNumber == expectedBlockNumber && burntFeesMap.contains(
              blockNumber)) {
          blockInfos.append((blockNumber, baseFeePerGas, gasUsed, timestamp))
          expectedBlockNumber += 1
        }
    }

    var accumulateFees = accumulateFees0

    val result = blockInfos.sortBy(_._1).map {
      case (blockNumber, baseFeePerGas, gasUsed, timestamp) =>
        val burntFees = burntFeesMap(blockNumber).hexToBigInt()
        accumulateFees += burntFees

        BlockBurns(blockNumber,
                   burntFees,
                   accumulateFees,
                   accumulateKlay,
                   timestamp)
    }

    blockPersistentAPI.insertBlockBurns(result)
  }

  def procBurnFee(): Unit = {
    val (maxBlockNumber, accumulateFees0, accumulateKlay) =
      blockPersistentAPI.getLatestBlockBurnInfo()

    var expectedBlockNumber = maxBlockNumber + 1 // process data only as far as the block is sequentially incremented

    val blockInfos = mutable.ArrayBuffer.empty[(Long, String, Int, Int)]

    blockPersistentAPI.findBlockFeeInfos(maxBlockNumber).sortBy(_._1).foreach {
      case (blockNumber, baseFeePerGas, gasUsed, timestamp) =>
        if (blockNumber == expectedBlockNumber) {
          blockInfos.append((blockNumber, baseFeePerGas, gasUsed, timestamp))
          expectedBlockNumber += 1
        }
    }

    var accumulateFees = accumulateFees0

    val result = blockInfos.sortBy(_._1).map {
      case (blockNumber, baseFeePerGas, gasUsed, timestamp) =>
        val burntFees = BigInt(baseFeePerGas) * gasUsed / 2
        accumulateFees += burntFees

        BlockBurns(blockNumber,
                   burntFees,
                   accumulateFees,
                   accumulateKlay,
                   timestamp)
    }

    blockPersistentAPI.insertBlockBurns(result)
  }
}
