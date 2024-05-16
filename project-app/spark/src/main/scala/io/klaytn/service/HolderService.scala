package io.klaytn.service

import io.klaytn._
import io.klaytn.client.{FinderRedis, SparkRedis}
import io.klaytn.contract.lib.{KIP17MetadataReader, KIP7MetadataReader}
import io.klaytn.dsl.db.withDB
import io.klaytn.model.finder.ContractType.{ERC1155, ERC721, KIP17, KIP37}
import io.klaytn.model.finder.{ContractType, TransferType}
import io.klaytn.persistent.{HolderPersistentAPI, TransferPersistentAPI}
import io.klaytn.repository._
import io.klaytn.utils.config.{Constants, FunctionSupport}
import io.klaytn.utils.klaytn.NumberConverter._
import io.klaytn.utils.gcs.GCSUtil
import io.klaytn.utils.spark.UserConfig
import io.klaytn.utils.SlackUtil
import io.klaytn.model.finder.NFTTransfer
import scala.collection.mutable
import org.apache.spark.sql.SparkSession

class HolderService(holderPersistentAPI: LazyEval[HolderPersistentAPI],
                    transferPersistentAPI: LazyEval[TransferPersistentAPI],
                    contractService: LazyEval[ContractService],
                    nftItemService: LazyEval[NFTItemService],
                    transferService: LazyEval[TransferService])
    extends Serializable {

  object NFTProcFlags extends Enumeration {

    /**
      * Managed in Redis, S3
      */
    val redis = ("HolderService:NFT:LastBlock")

    /**
      * Managed in RDS
      */
    val holder = ("HolderService:NFT:Holder:LastBlock")

    /**
      * Managed in RDS
      */
    val inventory = ("HolderService:NFT:Inventory:LastBlock")

    /**
      * Managed in Redis, S3
      */
    val tokenURI = ("HolderService:NFT:TokenURI:LastBlock")

    /**
      * Managed in Redis, S3
      */
    val account = ("HolderService:NFT:Account:LastBlock")
  }

  def saveToS3(key: String, data: String): Unit = {
    val s3Key =
      s"jobs/io.klaytn.apps.worker.FastWorkerStreaming/lastId/$key.${UserConfig.chainPhase.chain}"
    GCSUtil.writeText(UserConfig.baseBucket, s3Key, data)
  }

  def calAmount(m: mutable.Map[String, (BigInt, Int, Long)],
                key: String,
                amount: BigInt,
                ts: Int,
                blockNumber: Long): Unit = {
    val v = m.getOrElseUpdate(key, (BigInt(0), 0, 0L))
    val (oldAmount, oldTs, oldBlockNumber) = (v._1, v._2, v._3)

    val newAmount = oldAmount + amount
    val newTs = if (ts > oldTs) ts else oldTs
    val newBlockNumber =
      if (blockNumber > oldBlockNumber) blockNumber else oldBlockNumber

    m.put(key, (newAmount, newTs, newBlockNumber))
  }

  private def isZeroOrDead(address: String): Boolean = {
    address == Constants.ZeroAddress || address == Constants.DeadAddress
  }

  def procTokenHolder(): Unit = {
    val redisKey = "HolderService:Token:LastBlock"
    val tableId = SparkRedis.get(redisKey) match {
      case Some(info) => info.toLong
      case _          => return
    }

    val tokenTransfers =
      transferPersistentAPI.getTokenTransfers(tableId, 2000).sortBy(_.timestamp)
    if (tokenTransfers.isEmpty) return

    val contractAddresses = mutable.Set.empty[String]

    // key: contractAddress_[from or to]
    // value: amount_ts
    val m = mutable.Map.empty[String, (BigInt, Int, Long)]
    tokenTransfers.foreach { t =>
      val amount = t.amount.hexToBigInt()
      if (Constants.nonZeroOrDead(t.from)) {
        calAmount(m,
                  s"${t.contractAddress}\t${t.from}",
                  -amount,
                  t.timestamp,
                  t.blockNumber)
      }
      calAmount(m,
                s"${t.contractAddress}\t${t.to}",
                amount,
                t.timestamp,
                t.blockNumber)

      contractAddresses.add(t.contractAddress)
    }
    val holders = m.map {
      case (k, v) =>
        val s1 = k.split("\t")

        val (contractAddress, holder, amount, ts, blockNumber) =
          (s1(0), s1(1), v._1, v._2, v._3)

        TokenHolders(contractAddress, holder, amount, ts, blockNumber)
    }.toSeq

    holderPersistentAPI.insertTokenHolders(
      holders.filter(x => !isZeroOrDead(x.holderAddress)))

    contractAddresses.foreach { address: String =>
      contractService.checkContractAndUpdate(address, "token")
    }

    val accountTransferContracts = tokenTransfers.flatMap { t =>
      Seq(
        AccountTransferContracts(t.from,
                                 t.contractAddress,
                                 t.timestamp,
                                 TransferType.TOKEN),
        AccountTransferContracts(t.to,
                                 t.contractAddress,
                                 t.timestamp,
                                 TransferType.TOKEN)
      )
    }
    transferService.insertAccountTransferContracts(accountTransferContracts)

    SparkRedis.set(redisKey, s"${tokenTransfers.map(_.id).max}")
  }

  def procNFTHolder(): Unit = {
    val redisKey = "HolderService:NFT:LastBlock"
    val tableId = SparkRedis.get(redisKey) match {
      case Some(info) => info.toLong
      case _          => return
    }

    val nftTransfers =
      transferPersistentAPI.getNFTTransfers(tableId, 2000).sortBy(_.timestamp)

    if (nftTransfers.isEmpty) return

    // key: (contractAddress, tokenId), value: (totalTransfer, contractType, tokenUri)
//    val nftItems = mutable.Map.empty[(String, String), (Int, ContractType.Value, String)]

    val kip17Holders = nftTransfers
      .filter(x => x.contractType == KIP17 || x.contractType == ERC721)
      .flatMap { x =>
        Seq(
          NFTHolders(x.contractAddress,
                     x.from,
                     x.tokenId,
                     -x.tokenCount.hexToBigInt(),
                     "-",
                     x.timestamp,
                     x.blockNumber),
          NFTHolders(x.contractAddress,
                     x.to,
                     x.tokenId,
                     x.tokenCount.hexToBigInt(),
                     "-",
                     x.timestamp,
                     x.blockNumber)
        )
      }

    val kip17Inventories = nftTransfers
      .filter(x => x.contractType == KIP17 || x.contractType == ERC721)
      .flatMap { x =>
        // val uri = contractService.getTokenUri(x.contractType,
        //                                       x.contractAddress,
        //                                       x.tokenId)
        val uri = "-"

//      val v = nftItems.getOrElseUpdate((x.contractAddress, x.tokenId), (0, x.contractType, uri))
//      nftItems((x.contractAddress, x.tokenId)) = (v._1 + 1, x.contractType, uri)

        Seq(
          NFTInventories(isSend = true,
                         x.contractAddress,
                         x.from,
                         x.tokenId,
                         uri,
                         x.timestamp),
          NFTInventories(isSend = false,
                         x.contractAddress,
                         x.to,
                         x.tokenId,
                         uri,
                         x.timestamp)
        )
      }

    val kip37Holders = nftTransfers
      .filter(x => x.contractType == KIP37 || x.contractType == ERC1155)
      .flatMap { x =>
        val uri = "-"
        // val uri = contractService.getTokenUri(x.contractType,
        //                                       x.contractAddress,
        //                                       x.tokenId)

//      val v = nftItems.getOrElseUpdate((x.contractAddress, x.tokenId), (0, x.contractType, uri))
//      nftItems((x.contractAddress, x.tokenId)) = (v._1 + 1, x.contractType, uri)

        Seq(
          NFTHolders(x.contractAddress,
                     x.from,
                     x.tokenId,
                     -x.tokenCount.hexToBigInt(),
                     uri,
                     x.timestamp,
                     x.blockNumber),
          NFTHolders(x.contractAddress,
                     x.to,
                     x.tokenId,
                     x.tokenCount.hexToBigInt(),
                     uri,
                     x.timestamp,
                     x.blockNumber)
        )
      }

    kip17Holders
      .filter(x => !isZeroOrDead(x.holderAddress))
      .foreach(x => holderPersistentAPI.insertKIP17Holders(Seq(x)))
    kip37Holders
      .filter(x => !isZeroOrDead(x.holderAddress))
      .foreach(x => holderPersistentAPI.insertKIP37Holders(Seq(x)))
    kip17Inventories
      .filter(x => !isZeroOrDead(x.holderAddress))
      .foreach(x => holderPersistentAPI.insertKIP17Inventories(Seq(x)))

//    GCSUtil.writeText(
//      "klaytn-prod-spark",
//      s"output/fastworker/nftitems/${System.currentTimeMillis()}.step1.${FunctionSupport
//        .nftItems(UserConfig.chainPhase)}.$tableId.${nftTransfers.map(_.id.getOrElse(0L)).max}.${nftItems.size}",
//      s"${nftItems.toSeq.sortBy(x => (x._1._1, x._1._2.toLong)).mkString("\n")}"
//    )
//    if (FunctionSupport.nftItems(UserConfig.chainPhase)) {
//      nftItems.foreach {
//        case ((contractAddress, tokenId), (totalTransfer, contractType, tokenUri)) =>
//          nftItemService.updateTotalTransfer(contractAddress, contractType, tokenId, tokenUri, totalTransfer)
//      }
//    }

    val accountTransferContracts = nftTransfers.flatMap { t =>
      Seq(
        AccountTransferContracts(t.from,
                                 t.contractAddress,
                                 t.timestamp,
                                 TransferType.NFT),
        AccountTransferContracts(t.to,
                                 t.contractAddress,
                                 t.timestamp,
                                 TransferType.NFT)
      )
    }

    transferService.insertAccountTransferContracts(accountTransferContracts)

    SparkRedis.set(redisKey, s"${nftTransfers.map(_.id.getOrElse(0L)).max}")
  }

  /**
    * Call proc_nft_holder_aggregate
    * @deprecated
    * @param procNum
    */
  def procNFTHolderV2(procNum: Long): Unit = {
    try {
      // CALL PROCEDURE
      val isRunning = getIsRunning(NFTProcFlags.inventory)
      if (isRunning) {
        SlackUtil.sendMessage(s"procNFTHolderV2 is already running")
        return
      }
      withDB("finder03") { c =>
        val sql = s"CALL proc_nft_holder_aggregate(${procNum.toString()})"
        val stmt = c.prepareCall(sql)
        stmt.execute()
        stmt.close()
      }
    } catch {
      case e: Throwable => {
        SlackUtil.sendMessage(s"procNFTHolderV2 error: ${e.getMessage}")
        fixIsRunning(NFTProcFlags.holder)
      }
    }
  }

  /**
    * Call proc_nft_inven_aggregate
    * @deprecated
    * @param procNum
    */
  def procNFTInventories(procNum: Long): Unit = {
    try {
      // CALL PROCEDURE
      val isRunning = getIsRunning(NFTProcFlags.inventory)
      if (isRunning) {
        SlackUtil.sendMessage(s"procNFTInventories is already running")
        return
      }
      withDB("finder03") { c =>
        val sql = s"CALL proc_nft_inven_aggregate(${procNum.toString()})"
        val stmt = c.prepareCall(sql)
        stmt.execute()
        stmt.close()
      }
    } catch {
      case e: Throwable => {
        SlackUtil.sendMessage(s"procNFTInventories error: ${e.getMessage}")
        fixIsRunning(NFTProcFlags.holder)
      }
    }
  }

  /**
    * Set is_running flag to false
    * @deprecated
    * @param jobName
    */
  def fixIsRunning(jobName: String): Unit = {
    try {
      withDB("finder03") { c =>
        // 1. Check if is_running is true
        val sql =
          s"SELECT is_running FROM nft_aggregate_flag WHERE job_name = '${jobName}'"
        val stmt = c.prepareStatement(sql)
        val rs = stmt.executeQuery()
        var isRunning = false
        while (rs.next()) {
          isRunning = rs.getBoolean("is_running")
        }
        rs.close()
        stmt.close()

        if (isRunning) {
          // 2. If true, update is_running to false
          val sql =
            s"UPDATE nft_aggregate_flag SET is_running = false WHERE job_name = '${jobName}'"
          val stmt = c.prepareStatement(sql)
          stmt.executeUpdate()
          stmt.close()
        }
      }
      SlackUtil.sendMessage(s"fixIsRunning: ${jobName} is fixed")
    } catch {
      case e: Throwable => {
        SlackUtil.sendMessage(s"fixIsRunning: ${jobName} ${e.getMessage}")
      }
    }

  }

  /**
    * Get is_running from nft_aggregate_flag
    * @deprecated
    * @param jobName
    * @return
    */
  def getIsRunning(jobName: String): Boolean = {
    var isRunning = false
    try {
      withDB("finder03") { c =>
        val sql =
          s"SELECT is_running FROM nft_aggregate_flag WHERE job_name = '${jobName}'"
        val stmt = c.prepareStatement(sql)
        val rs = stmt.executeQuery()
        if (rs.next()) {
          isRunning = rs.getBoolean("is_running")
        }
        rs.close()
        stmt.close()
      }
    } catch {
      case e: Throwable => {
        SlackUtil.sendMessage(s"getIsRunning: ${jobName} ${e.getMessage}")
      }
    }
    isRunning
  }

  /**
    * Insert account transfer contracts
    *
    * @param procNum
    * @return
    */
  def procAccountTransferContracts(procNum: Long): Long = {
    var startId = SparkRedis.get(NFTProcFlags.account) match {
      case Some(info) => info.toLong
      case _          => 0L
    }
    val nftTransfers = mutable.ArrayBuffer.empty[NFTTransfer]
    transferPersistentAPI
      .getNFTTransfersWithoutRetry(startId, procNum.toInt)
      .sortBy(_.timestamp)
      .foreach { t =>
        nftTransfers += t
      }
    if (nftTransfers.size == 0) {
      val latestId = getLatestTransferId()
      // Has next
      if (startId + procNum < latestId) {
        // Try until get data
        while (nftTransfers.isEmpty) {
          startId = startId + procNum
          if (startId >= latestId) {
            // Proc is done
            SparkRedis.set(NFTProcFlags.account, s"${latestId}")
            return latestId
          }
          transferPersistentAPI
            .getNFTTransfersWithoutRetry(startId, procNum.toInt)
            .sortBy(_.timestamp)
            .foreach { t =>
              nftTransfers += t
            }
        }
      } else {
        // Proc is done
        SparkRedis.set(NFTProcFlags.account, s"${latestId}")
        return latestId
      }
    }

    // Make two account transfer contracts for each transfer
    val accountTransferContracts = nftTransfers.flatMap { t =>
      Seq(
        AccountTransferContracts(t.from,
                                 t.contractAddress,
                                 t.timestamp,
                                 TransferType.NFT),
        AccountTransferContracts(t.to,
                                 t.contractAddress,
                                 t.timestamp,
                                 TransferType.NFT)
      )
    }
    transferService.insertAccountTransferContracts(accountTransferContracts)
    val lastId = nftTransfers.map(_.id.getOrElse(0L)).max
    SparkRedis.set(NFTProcFlags.account, s"${lastId}")
    lastId
  }

  def getLatestTransferId(): Long = {
    withDB("finder03") { c =>
      {
        val sql =
          s"SELECT MAX(id) AS id FROM nft_transfers;"
        val stmt = c.prepareStatement(sql)
        val rs = stmt.executeQuery()
        var id = 0L
        if (rs.next()) {
          id = rs.getLong("id")
        }
        rs.close()
        stmt.close()
        id
      }
    }
  }

  /**
    * Get Token URI from caver and update tokenURI in nft_inventories
    * @param procNum
    * @return
    */
  def procNFTTokenURI(procNum: Long): Long = {
    var startId = SparkRedis.get(NFTProcFlags.tokenURI) match {
      case Some(info) => info.toLong
      case _          => 0L
    }
    val nftTransfers = mutable.ArrayBuffer.empty[NFTTransfer]
    transferPersistentAPI
      .getNFTTransfersWithoutRetry(startId, procNum.toInt)
      .sortBy(_.timestamp)
      .foreach { t =>
        nftTransfers += t
      }
    if (nftTransfers.isEmpty) {
      val latestId = getLatestTransferId()
      // Has next
      if (startId + procNum < latestId) {
        // Try until get data
        while (nftTransfers.isEmpty) {
          startId = startId + procNum
          if (startId >= latestId) {
            // Proc is done
            SparkRedis.set(NFTProcFlags.tokenURI, s"${latestId}")
            return latestId
          }
          transferPersistentAPI
            .getNFTTransfersWithoutRetry(startId, procNum.toInt)
            .sortBy(_.timestamp)
            .foreach { t =>
              nftTransfers += t
            }
        }
      } else {
        // Proc is done
        SparkRedis.set(NFTProcFlags.tokenURI, s"${latestId}")
        return latestId
      }
    }
    // Get last id
    val endId = nftTransfers.map(_.id.getOrElse(0L)).max
    // (contractAddress, tokenId, contractType) -> tokenURI
    val tokens =
      mutable.HashMap.empty[(String, String, ContractType.Value), String]
    withDB("finder03") { c =>
      val pstmt = c.prepareStatement(s"""
        SELECT
        ni.contract_address,
        ni.token_id,
        ni.contract_type,
        ni.token_uri
        FROM
          nft_inventories ni
          RIGHT JOIN (
            SELECT
              ntv2.contract_address,
              ntv2.token_id,
              ntv2.from AS holder_address
            FROM
              nft_transfers ntv2
            WHERE
              ntv2.id BETWEEN ${startId} AND ${endId}
            GROUP BY
              ntv2.contract_address,
              ntv2.token_id,
              ntv2.from
            UNION
            SELECT
              ntv2.contract_address,
              ntv2.token_id,
              ntv2.to AS holder_address
            FROM
              nft_transfers ntv2
            WHERE
              ntv2.id BETWEEN ${startId} AND ${endId}
            GROUP BY
              ntv2.contract_address,
              ntv2.token_id,
              ntv2.to) AS t ON ni.contract_address = t.contract_address
            AND ni.token_id = t.token_id
            AND ni.holder_address = t.holder_address
          where ni.token_uri = '-'
          group by ni.contract_address, ni.token_id
    """)
      val rs = pstmt.executeQuery()
      while (rs.next()) {
        val contractAddress = rs.getString(1)
        val tokenId = rs.getString(2)
        val contractType = ContractType.from(rs.getInt(3))
        val tokenUri = rs.getString(4)
        tokens.put((contractAddress, tokenId, contractType), tokenUri)
      }
      rs.close()
      pstmt.close()
    }
    if (tokens.isEmpty) {
      SparkRedis.set(NFTProcFlags.tokenURI, s"${endId}")
      return endId
    }
    tokens.foreach(x => {
      val contractAddress = x._1._1
      val tokenId = BigInt(x._1._2)
      val contractType = x._1._3
      // Max length is 255 (varchar)
      val newTokenURI =
        contractService.getFreshTokenURI(contractType, contractAddress, tokenId)
      if (newTokenURI != null && newTokenURI.length <= 255) {
        tokens.put((contractAddress, tokenId.toString, contractType),
                   newTokenURI)
      }
    })
    // (contractAddress, tokenId, tokenUri)
    val updateTokenList =
      tokens.filter(_._2 != "-").map(x => (x._1._1, x._1._2, x._2)).toList
    val len = updateTokenList.length
    if (len > 0) {
      val updateTokenListChunk = updateTokenList.grouped(1000).toList
      updateTokenListChunk.foreach { x =>
        holderPersistentAPI.updateTokenUriBulk(x)
      }
    }
    SparkRedis.set(NFTProcFlags.tokenURI, s"${endId}")
    return endId
  }

  /**
    * @deprecated
    * @param diff
    * @param procNum
    */
  def catchUpNFTHolder(diff: Long, procNum: Long) = {
    val chunk = (diff / (procNum * 10)).floor.abs.toInt
    if (chunk >= 1) {
      Range.inclusive(1, chunk).foreach { x =>
        procNFTHolderV2(procNum)
      }
    } else {
      procNFTHolderV2(diff)
    }
  }

  /**
    * @deprecated
    * @param diff
    * @param procNum
    */
  def catchUpNFTInventories(diff: Long, procNum: Long) = {
    val chunk = (diff / (procNum * 10)).floor.abs.toInt
    if (chunk >= 1) {
      Range.inclusive(1, chunk).foreach { x =>
        procNFTInventories(procNum)
      }
    } else {
      procNFTInventories(diff)
    }
  }

  /**
    * @deprecated
    * @param startId
    * @param diff
    * @param procNum
    * @return
    */
  def catchUpAccountTransferContracts(startId: Long,
                                      diff: Long,
                                      procNum: Long) = {
    val chunk = (diff / procNum).floor.abs.toInt
    if (chunk >= 1) {
      Range.inclusive(1, chunk).foreach { x =>
        procAccountTransferContracts(procNum)
      }
    } else {
      procAccountTransferContracts(diff)
    }
  }

  def procNFT(procNum: Long = 2000L): Unit = {
    val logs = mutable.Map.empty[String, String]

    try {
      // 1. Insert AccountTransferContracts
      val s1 = System.currentTimeMillis()
      logs.put("Start procAccountTransferContracts", s"${procNum}")
      procAccountTransferContracts(procNum)
      val s2 = System.currentTimeMillis()
      logs.put("Finish procAccountTransferContracts", s"${s2 - s1} ms")

      // 2. TokenURI
      val invProc = getNFTAggregateFlag(NFTProcFlags.inventory).toLong
      val tkProc = getNFTAggregateFlag(NFTProcFlags.tokenURI).toLong
      // Token Process should be done after Inventory Process
      if (invProc > tkProc) {
        logs.put("Start procNFTTokenURI", s"${invProc - tkProc}")
        procNFTTokenURI(invProc - tkProc)
      } else {
        logs.put("procNFTTokenURI skipped", s"${tkProc - invProc}")
      }
      val s3 = System.currentTimeMillis()
      logs.put("Finish procNFTTokenURI", s"${s3 - s2} ms")

      // 3. Get the states -> Redis == DB == s3
      logs.put("Start procNFTStates", s"${procNum}")
      val inventoryProc = getNFTAggregateFlag(NFTProcFlags.inventory).toLong
      val holderProc = getNFTAggregateFlag(NFTProcFlags.holder).toLong
      val accountProc = getNFTAggregateFlag(NFTProcFlags.account).toLong
      val tokenURIProc = getNFTAggregateFlag(NFTProcFlags.tokenURI).toLong
      val redisProc = getNFTAggregateFlag(NFTProcFlags.redis).toLong

      var s4 = System.currentTimeMillis()
      if (accountProc > redisProc) {
        logs.put("Setting new Redis proc", s"${redisProc} => ${accountProc}")
        SparkRedis.set(NFTProcFlags.redis, accountProc.toString())
        s4 = System.currentTimeMillis()
      } else if (accountProc <= redisProc) {
        logs.put("Redis is up to date",
                 s"accountProc(${accountProc}) <= redisProcId(${redisProc})")
      }
      logs.put("Finish procNFTStates", s"${s4 - s3} ms")
      logs.put("Total time", s"${s4 - s1} ms")

      val logString = logs.map(x => x._1 + ": " + x._2).mkString("\n")
      val message =
        s"""[ProcNFT logs]\n${logString}\n\n[ProcNFT states]\n inventoryProc: ${inventoryProc}\n holderProc: ${holderProc}\n accountProc: ${accountProc} \n tokenURIProc: ${tokenURIProc}\n redisProc: ${redisProc}\n newRedisProc: ${List(
          accountProc,
          redisProc).max.toString()}"""
      SlackUtil.sendMessage(message)
    } catch {
      case e: Exception =>
        SlackUtil.sendMessage(
          s"procNFT() Error! ${e.getMessage} ${e.getStackTrace.mkString("\n")}")
        Thread.sleep(5000)
    }
  }

  def getNFTAggregateFlag(jobName: String): Long = {
    jobName match {
      case NFTProcFlags.holder    => getNFTAggregateFlagFromDB(jobName)
      case NFTProcFlags.inventory => getNFTAggregateFlagFromDB(jobName)
      case _                      => getNFTAggregateFlagFromRedis(jobName)
    }
  }

  def getNFTAggregateFlagFromDB(jobName: String): Long = {
    withDB("finder03") { c =>
      val pstmt = c.prepareStatement(
        s"SELECT last_id from nft_aggregate_flag where job_name = '${jobName}'")
      val rs = pstmt.executeQuery()
      pstmt.close()
      var result = 0L
      if (rs.next()) {
        result = rs.getLong(1)
        rs.close()
      }
      result
    }
  }

  def getNFTAggregateFlagFromRedis(jobName: String): Long = {
    SparkRedis.get(jobName) match {
      case Some(info) => info.toLong
      case _          => 0L
    }
  }

  def checkNFTAggregateFlagDBSame(): Boolean = {
    val lastProcIdMap = mutable.Map.empty[String, Long]
    withDB("finder03") { c =>
      val pstmt = c.prepareStatement(
        s"SELECT job_name, last_id from nft_aggregate_flag where job_name in ('${NFTProcFlags.holder}', '${NFTProcFlags.inventory}')")
      val rs = pstmt.executeQuery()
      while (rs.next()) {
        lastProcIdMap.put(rs.getString(1), rs.getLong(2))
      }
      rs.close()
      pstmt.close()
    }
    lastProcIdMap.size == 2 && lastProcIdMap(NFTProcFlags.holder) == lastProcIdMap(
      NFTProcFlags.inventory)
  }

  def checkNFTAggregateFlagInitalized(): Boolean = {
    withDB("finder03") { c =>
      val pstmt = c.prepareStatement(
        s"SELECT count(*) from nft_aggregate_flag where job_name in ('${NFTProcFlags.holder}', '${NFTProcFlags.inventory}')")
      val count = pstmt.executeQuery().getInt(1)
      pstmt.close()
      count == 2
    }
  }
  def initializeNFTAggregateFlag() = {
    withDB("finder03") { c =>
      val pstmt = c.prepareStatement(
        s"INSERT IGNORE INTO nft_aggregate_flag (job_name, last_id, is_running) " +
          s"VALUES ('${NFTProcFlags.holder}', 0, 0)" +
          s"('${NFTProcFlags.inventory}', 0, 0)")
      pstmt.executeUpdate()
    }
  }

  def updateHolderCount(): Unit = {
    val holderCount = mutable.Map.empty[String, Long]

    withDB("finder03") { c =>
      val pstmt = c.prepareStatement(
        "select contract_address, count(*) from nft_holders group by contract_address")
      val rs = pstmt.executeQuery()
      while (rs.next()) {
        holderCount.put(rs.getString(1), rs.getLong(2))
      }
      rs.close()
      pstmt.close()

      val pstmt2 =
        c.prepareStatement(s"""select contract_address, sum(cnt) from (
                                         |select contract_address, token_id, count(*) cnt from nft_inventories
                                         |where contract_type = 3 and holder_address != '${Constants.ZeroAddress}'
                                         |group by contract_address, token_id) a group by contract_address""".stripMargin)
      val rs2 = pstmt2.executeQuery()
      while (rs2.next()) {
        holderCount.put(rs2.getString(1), rs2.getLong(2))
      }
      rs2.close()
      pstmt2.close()
    }

    holderCount.grouped(40).foreach { grouped =>
      withDB(ContractRepository.ContractDB) { c =>
        val pstmt =
          c.prepareStatement(
            "UPDATE `contracts` SET `holder_count`=? WHERE `contract_address`=?")
        grouped.foreach {
          case (k, v) =>
            pstmt.setLong(1, v)
            pstmt.setString(2, k)
            pstmt.execute()
        }
        pstmt.close()
      }
      Thread.sleep(100)
    }
  }
  def procTokenURI(spark: SparkSession): Unit = {
    val redisKey = "HolderService:TokenURI:LastBlock"
    val tableId = SparkRedis.get(redisKey) match {
      case Some(info) => info.toLong
      case _          => return
    }

    val nftTransfers =
      transferPersistentAPI.getNFTTransfers(tableId, 2000).sortBy(_.timestamp)

    if (nftTransfers.isEmpty) return
    // Parrallelize
    val nftTransfersRDD = spark.sparkContext.parallelize(nftTransfers, 20)
    nftTransfersRDD.foreachPartition { partition =>
      try {
        val tokenURIs = partition
          .map { (x) =>
            val contractAddress = x.contractAddress
            val tokenId = x.tokenId
            val tokenURI =
              contractService.getFreshTokenURI(
                x.contractType,
                contractAddress,
                tokenId
              )
            (contractAddress, tokenId.toString, tokenURI)
          }
          .toSeq
          .filter(_._3 != "-")
        if (tokenURIs.nonEmpty)
          holderPersistentAPI.updateTokenUriBulk(tokenURIs)
      } catch {
        case e: Exception =>
          SlackUtil.sendMessage(
            s"procTokenURI() Error! ${e.getMessage} ${e.getStackTrace.mkString("\n")}")
          throw e
      }
    }
    SparkRedis.set(redisKey, s"${nftTransfers.map(_.id.getOrElse(0L)).max}")
  }

  def procTokenBurnAmount(): Unit = {
    val redisKey = "HolderService:TokenBurn:LastBlock"
    val tableId = SparkRedis.get(redisKey) match {
      case Some(info) => info.toLong
      case _          => return
    }

    val burns =
      transferPersistentAPI.getTokenBurns(tableId, 2000).sortBy(_.timestamp)
    if (burns.isEmpty) return

    val result = mutable.Map.empty[String, (BigInt, Int)]

    burns.foreach { tokenBurn =>
      val (amount0, count0) =
        result.getOrElseUpdate(tokenBurn.contractAddress, (BigInt(0), 0))
      result(tokenBurn.contractAddress) =
        (amount0 + tokenBurn.amount.hexToBigInt(), count0 + 1)
    }

    result.foreach {
      case (contractAddress, (amount, count)) =>
        contractService.updateBurnInfo(contractAddress, amount, count)
    }

    SparkRedis.set(redisKey, s"${burns.map(_.id).max}")
  }

  def procNFTBurnAmount(): Unit = {
    val redisKey = "HolderService:NFTBurn:LastBlock"
    val tableId = SparkRedis.get(redisKey) match {
      case Some(info) => info.toLong
      case _          => return
    }

    val burns =
      transferPersistentAPI.getNFTBurns(tableId, 2000).sortBy(_.timestamp)
    if (burns.isEmpty) return

    val result = mutable.Map.empty[String, (BigInt, Int)]

    // key: (contractAddress, tokenId, contractType), value: (burnAmount, totalBurns)
    val nftItems =
      mutable.Map.empty[(String, BigInt, ContractType.Value), (BigInt, Int)]

    burns.foreach { nftBurn =>
      val (amount0, count0) =
        result.getOrElseUpdate(nftBurn.contractAddress, (BigInt(0), 0))
      result(nftBurn.contractAddress) =
        (amount0 + nftBurn.tokenCount.hexToBigInt(), count0 + 1)

      val (amount1, count1) =
        nftItems.getOrElseUpdate(
          (nftBurn.contractAddress, nftBurn.tokenId, nftBurn.contractType),
          (BigInt(0), 0))

      nftItems(
        (nftBurn.contractAddress, nftBurn.tokenId, nftBurn.contractType)) =
        (amount1 + nftBurn.tokenCount.hexToBigInt(), count1 + 1)
    }

    result.foreach {
      case (contractAddress, (amount, count)) =>
        contractService.updateBurnInfo(contractAddress, amount, count)
    }

    if (FunctionSupport.nftItems(UserConfig.chainPhase)) {
      nftItems.foreach {
        case ((contractAddress, tokenId, contractType),
              (burnAmount, totalBurns)) =>
          nftItemService.updateBurn(contractAddress,
                                    contractType,
                                    tokenId,
                                    burnAmount,
                                    totalBurns)
      }
    }

    SparkRedis.set(redisKey, s"${burns.map(_.id.getOrElse(0L)).max}")
  }

  private def enqueueCorrectTokenHolder(queueRedisKey: String,
                                        dequeueData: Seq[(String, String)],
                                        nowScore: Long): Unit = {
    val tokenHolderTableId =
      SparkRedis.get("HolderService:Token:LastBlock") match {
        case Some(info) => info.toLong
        case _          => return
      }

    val redisKey = "HolderService:CorrectTokenHolder:LastBlock"
    val tableId = SparkRedis.get(redisKey) match {
      case Some(info) => Math.min(tokenHolderTableId, info.toLong)
      case _          => tokenHolderTableId
    }

    val tokenTransfers = transferPersistentAPI.getTokenTransfers(tableId, 1000)
    if (tokenTransfers.isEmpty) return

    tokenTransfers
      .flatMap(t => Seq((t.contractAddress, t.from), (t.contractAddress, t.to)))
      .filter(t => !isZeroOrDead(t._2))
      .filter(k => !dequeueData.contains(k))
      .distinct
      .foreach(k =>
        SparkRedis.zaddNX(queueRedisKey, nowScore, s"${k._1}\t${k._2}"))

    SparkRedis.set(redisKey, s"${tokenTransfers.map(_.id).max}")
  }

  def procCorrectTokenHolder(): Unit = {
    val queueRedisKey = "CorrectHolderQueue"
    val nowScore = System.currentTimeMillis() / 1000

    val collectTime = 30
    val maxScore = nowScore - collectTime
    val getCount = 100
    val dequeueData =
      SparkRedis.zrangebyscore(queueRedisKey, 0, maxScore, 0, getCount).map {
        d =>
          val s = d.split("\t")
          (s(0), s(1))
      }

    val kip7 = new KIP7MetadataReader(CaverFactory.caver)

    dequeueData
      .foreach {
        case (contractAddress, holderAddress) =>
          val (dbAmount, id) =
            holderPersistentAPI
              .getTokenBalanceAndId(contractAddress, holderAddress)
              .getOrElse((BigInt(0), 0L))

          try {
            val chainAmount = kip7
              .balanceOf(contractAddress, holderAddress)
              .getOrElse(BigInt(0))

            if (chainAmount != dbAmount) {
              holderPersistentAPI.updateTokenBalance(contractAddress,
                                                     holderAddress,
                                                     chainAmount)
              holderPersistentAPI.insertCorrectHolderHistory(contractAddress,
                                                             holderAddress,
                                                             chainAmount,
                                                             dbAmount)
              FinderRedis.del(s"cache/token-holder::$id")
            }
          } catch { case _: Throwable => }
      }

    // If the fetched data is less than getcount, delete it as a range, and if it is more than getcount, delete it one by one.
    if (dequeueData.size < getCount)
      SparkRedis.zremrangebyscore(queueRedisKey, 0, maxScore)
    else
      SparkRedis.zrem(queueRedisKey, dequeueData.map(d => s"${d._1}\t${d._2}"))

    contractService.updateHolderCount(dequeueData.map(_._1).distinct,
                                      isToken = true)

    enqueueCorrectTokenHolder(queueRedisKey, dequeueData, nowScore)
  }

  private def enqueueCorrectNFTHolder(queueRedisKey: String,
                                      dequeueData: Seq[(String, String)],
                                      nowScore: Long): Unit = {
    val nftHolderTableId = SparkRedis.get("HolderService:NFT:LastBlock") match {
      case Some(info) => info.toLong
      case _          => return
    }

    val redisKey = "HolderService:CorrectNFTHolder:LastBlock"
    val tableId = SparkRedis.get(redisKey) match {
      case Some(info) => Math.min(nftHolderTableId, info.toLong)
      case _          => nftHolderTableId
    }

    val nftTransfers = transferPersistentAPI.getNFTTransfers(tableId, 1000)
    if (nftTransfers.isEmpty) return

    nftTransfers
      .flatMap(t => Seq((t.contractAddress, t.from), (t.contractAddress, t.to)))
      .filter(t => !isZeroOrDead(t._2))
      .filter(k => !dequeueData.contains(k))
      .distinct
      .foreach(k =>
        SparkRedis.zaddNX(queueRedisKey, nowScore, s"${k._1}\t${k._2}"))

    SparkRedis.set(redisKey, s"${nftTransfers.map(_.id.get).max}")
  }

  def procCorrectNFTHolder(): Unit = {
    val queueRedisKey = "CorrectHolderQueue:NFT"
    val nowScore = System.currentTimeMillis() / 1000

    val collectTime = 30
    val maxScore = nowScore - collectTime
    val getCount = 100
    val dequeueData =
      SparkRedis.zrangebyscore(queueRedisKey, 0, maxScore, 0, getCount).map {
        d =>
          val s = d.split("\t")
          (s(0), s(1))
      }

    val kip17 = new KIP17MetadataReader(CaverFactory.caver)

    val contractMap = dequeueData
      .map(_._1)
      .distinct
      .flatMap(contractAddress => contractService.findContract(contractAddress))
      .filter(c => c.contractType == KIP17 || c.contractType == ERC721)
      .map(c => (c.contractAddress, c))
      .toMap

    dequeueData
      .foreach {
        case (contractAddress, holderAddress) =>
          val (dbAmount, id) =
            holderPersistentAPI
              .getNFTBalanceAndId(contractAddress, holderAddress)
              .getOrElse((BigInt(0), 0L))

          try {
            val chainAmount =
              if (contractMap.contains(contractAddress))
                kip17
                  .balanceOf(contractAddress, holderAddress)
                  .getOrElse(BigInt(0))
              else
                throw new RuntimeException(
                  s"'$contractAddress' is not kip17(erc721).")

            if (chainAmount != dbAmount) {
              holderPersistentAPI.updateNFTBalance(contractAddress,
                                                   holderAddress,
                                                   chainAmount)
              holderPersistentAPI.insertCorrectHolderHistory(contractAddress,
                                                             holderAddress,
                                                             chainAmount,
                                                             dbAmount)
              FinderRedis.del(s"cache/nft-holder::$id")
            }
          } catch {
            case _: Throwable =>
          }
      }

    // If the fetched data is less than getcount, delete it as a range, and if it is more than getcount, delete it one by one.
    if (dequeueData.size < getCount)
      SparkRedis.zremrangebyscore(queueRedisKey, 0, maxScore)
    else
      SparkRedis.zrem(queueRedisKey, dequeueData.map(d => s"${d._1}\t${d._2}"))

    contractService.updateHolderCount(dequeueData.map(_._1).distinct,
                                      isToken = false)

    enqueueCorrectNFTHolder(queueRedisKey, dequeueData, nowScore)
  }
}
