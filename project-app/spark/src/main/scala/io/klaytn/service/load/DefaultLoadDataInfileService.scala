package io.klaytn.service.load

import io.klaytn.dsl.db.withDB
import io.klaytn.model._
import io.klaytn.repository.{
  BlockRepository,
  EventLogRepository,
  TransactionRepository,
  InternalTransactionRepository
}
import io.klaytn.service.LoadDataInfileService
import io.klaytn.utils.gcs.GCSUtil
import io.klaytn.utils.spark.UserConfig
import io.klaytn.utils.{JsonUtil, SlackUtil}
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.exception.ExceptionUtils
import java.io.{File}

import java.text.SimpleDateFormat
import java.util.TimeZone
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class DefaultLoadDataInfileService extends LoadDataInfileService {
  private val FieldDelim = "\t"

  private val Nil = "NIL"

  private val df = {
    val df = new SimpleDateFormat("yyyyMM")
    df.setTimeZone(TimeZone.getTimeZone("Asia/Seoul"))
    df
  }

  def downloadTempFile(bucket: String, key: String): String = {
    val tempFile = s"/tmp/${Random.nextInt(10000000)}-${key.split("/").last}"
    GCSUtil.download(bucket, key, tempFile)
    tempFile
  }

  def deleteTempFile(tempFile: String): Unit = {
    new File(tempFile).delete()
  }

  def s3PathToTempFile(s3Path: String): String = {
    val bucket = s3Path.split("/").head
    val key = s3Path.split("/").tail.mkString("/")
    downloadTempFile(bucket, key)
  }

  override def writeLoadData(jobBasePath: String,
                             typ: String,
                             blockNumber: Long,
                             content: Seq[String]): Option[String] = {
    if (content.isEmpty) {
      return None
    }

    val bucket = UserConfig.baseBucket
    val rand = Random.nextInt(10000000)

    val savePath =
      s"$jobBasePath/loadDataFromS3-${UserConfig.chainPhase}-$typ-$rand.$blockNumber"
    GCSUtil.writeText(bucket, savePath, s"${content.mkString("\n")}\n")

    Some(savePath)
  }

  override def loadDataAndDeleteFile(savePath: String,
                                     dbName: Option[String]): Unit = {
    val bucket = UserConfig.baseBucket

    if (!GCSUtil.exist(bucket, savePath)) {
      SlackUtil.sendMessage(s"cannot find file: $bucket/$savePath")
      return
    }

    try {
      if (savePath.contains("-block-")) {
        loadDataFromS3Block(s"$bucket/$savePath")
      } else if (savePath.contains("-tx-")) {
        loadDataFromS3Transaction(s"$bucket/$savePath")
      } else if (savePath.contains("-eventlog-")) {
        loadDataFromS3EventLog(s"$bucket/$savePath")
      } else if (savePath.contains("-itx-")) {
        loadDataFromS3InternalTransaction(s"$bucket/$savePath", dbName.get)
      } else if (savePath.contains("-itxindex-")) {
        loadDataFromS3InternalTransactionIndex(s"$bucket/$savePath", dbName.get)
      }
    } catch {
      case e: Throwable =>
        SlackUtil.sendMessage(s"""$bucket/$savePath
                                 |${e.getMessage}
                                 |${StringUtils.abbreviate(
                                   ExceptionUtils.getStackTrace(e),
                                   500)}
                                 |""".stripMargin)
    }

    GCSUtil.delete(bucket, savePath, false)
  }

  override def blockLine(block: RefinedBlock): String = {
    val blockResult = ArrayBuffer.empty[String]

    block.blockScore match {
      case Some(blockScope) => blockResult.append(blockScope.toString)
      case _                => blockResult.append(Nil)
    }

    blockResult.append(block.extraData)
    blockResult.append(block.gasUsed.toString)
    blockResult.append(block.governanceData.getOrElse(Nil))
    blockResult.append(block.hash)
    blockResult.append(block.logsBloom)
    blockResult.append(block.number.toString)
    blockResult.append(block.parentHash)
    blockResult.append(block.proposer.getOrElse(Nil))
    blockResult.append(block.receiptsRoot)
    blockResult.append(block.reward)
    blockResult.append(block.size.toString)
    blockResult.append(block.stateRoot)
    blockResult.append(block.timestamp.toString)
    blockResult.append(block.timestampFoS.toString)
    blockResult.append(block.totalBlockScore.toString)
    blockResult.append(block.transactionCount.toString)
    blockResult.append(block.transactionsRoot)
    blockResult.append(block.voteData)
    blockResult.append(df.format(block.timestamp * 1000L))
    blockResult.append(block.baseFeePerGas.getOrElse(Nil))

    blockResult.mkString(FieldDelim)
  }

  override def transactionReceiptLines(block: Block): Seq[String] =
    this.transactionReceiptLines(block.toRefined._2)

  override def transactionReceiptLines(
      transactionReceipts: List[RefinedTransactionReceipt]): Seq[String] = {
    import io.klaytn.utils.JsonUtil.Implicits._

    val result = ArrayBuffer.empty[String]

    transactionReceipts.foreach { transactionReceipt =>
      val txResult = ArrayBuffer.empty[String]

      txResult.append(transactionReceipt.blockHash)
      txResult.append(transactionReceipt.blockNumber.toString)
      txResult.append(transactionReceipt.codeFormat.getOrElse(Nil))
      txResult.append(transactionReceipt.contractAddress.getOrElse(Nil))
      txResult.append(transactionReceipt.feePayer.getOrElse(Nil))
      transactionReceipt.feePayerSignatures match {
        case Some(feePayerSignatures) =>
          txResult.append(JsonUtil.asJson(feePayerSignatures))
        case _ => txResult.append(Nil)
      }
      txResult.append(transactionReceipt.feeRatio.getOrElse(Nil))
      txResult.append(transactionReceipt.from)
      txResult.append(transactionReceipt.gas.toString)
      txResult.append(transactionReceipt.gasPrice)
      txResult.append(transactionReceipt.gasUsed.toString)
      transactionReceipt.humanReadable match {
        case Some(humanReadable) =>
          txResult.append(if (humanReadable) "1" else "0")
        case _ => txResult.append(Nil)
      }
      txResult.append(transactionReceipt.input.getOrElse(Nil))
      txResult.append(transactionReceipt.key.getOrElse(Nil))
      txResult.append(transactionReceipt.logsBloom)
      txResult.append(transactionReceipt.nftTransferCount.toString)
      txResult.append(transactionReceipt.nonce.toString)
      txResult.append(transactionReceipt.senderTxHash)
      txResult.append(JsonUtil.asJson(transactionReceipt.signatures))
      txResult.append(if (transactionReceipt.status) "1" else "0")
      txResult.append(transactionReceipt.timestamp.toString)
      txResult.append(transactionReceipt.to.getOrElse(Nil))
      txResult.append(transactionReceipt.tokenTransferCount.toString)
      txResult.append(transactionReceipt.transactionHash)
      txResult.append(transactionReceipt.transactionIndex.toString)
      transactionReceipt.txError match {
        case Some(txError) => txResult.append(txError.toString)
        case _             => txResult.append(Nil)
      }
      txResult.append(transactionReceipt.`type`)
      txResult.append(transactionReceipt.typeInt.toString)
      txResult.append(transactionReceipt.value.getOrElse(Nil))
      transactionReceipt.accessList match {
        case Some(accessList) => txResult.append(JsonUtil.asJson(accessList))
        case _                => txResult.append(Nil)
      }
      txResult.append(transactionReceipt.chainId.getOrElse(Nil))
      txResult.append(transactionReceipt.maxFeePerGas.getOrElse(Nil))
      txResult.append(transactionReceipt.maxPriorityFeePerGas.getOrElse(Nil))
      txResult.append(transactionReceipt.effectiveGasPrice.getOrElse(Nil))

      result.append(txResult.mkString(FieldDelim))
    }

    result
  }

  override def eventLogLines(eventLogs: List[RefinedEventLog]): Seq[String] = {
    val result = ArrayBuffer.empty[String]

    eventLogs.foreach { eventLog =>
      val logResult = ArrayBuffer.empty[String]
      logResult.append(eventLog.address)
      logResult.append(eventLog.blockHash)
      logResult.append(eventLog.blockNumber.toString)
      logResult.append(
        if (eventLog.topics.isEmpty) "" else eventLog.topics.head)
      logResult.append(eventLog.data)
      logResult.append(eventLog.logIndex.toString)
      logResult.append(JsonUtil.asJson(eventLog.topics))
      logResult.append(eventLog.transactionHash)
      logResult.append(eventLog.transactionIndex.toString)
      eventLog.removed match {
        case Some(removed) => logResult.append(if (removed) "1" else "0")
        case _             => logResult.append(Nil)
      }

      result.append(logResult.mkString(FieldDelim))
    }

    result
  }

  override def internalTransactionLines(itxs: List[RefinedInternalTransactions])
    : (Seq[String], Seq[(String, String)]) = {
    import io.klaytn.utils.JsonUtil.Implicits._

    val result1 = ArrayBuffer.empty[String]
    val result2 = ArrayBuffer.empty[(String, String)]

    itxs
      .filter { x =>
        val from = x.from.getOrElse("")
        from.nonEmpty && from.length > 2
      }
      .foreach { itx =>
        val resultITX = ArrayBuffer.empty[String]
        val resultITXIndex = ArrayBuffer.empty[String]
        val resultITXIndex2 = ArrayBuffer.empty[String]

        resultITX.append(itx.blockNumber.toString)
        resultITX.append(itx.callId.toString)
        resultITX.append(itx.error.getOrElse(Nil))
        resultITX.append(itx.from.get)
        resultITX.append(itx.gas.getOrElse(0).toString)
        itx.gasUsed match {
          case Some(x) => resultITX.append(x.toString)
          case _       => resultITX.append(Nil)
        }
        resultITX.append(itx.index.toString)
        resultITX.append(itx.input.getOrElse(Nil))
        resultITX.append(itx.output.getOrElse(Nil))
        itx.parentCallId match {
          case Some(x) => resultITX.append(x.toString)
          case _       => resultITX.append(Nil)
        }
        itx.reverted match {
          case Some(x) => resultITX.append(JsonUtil.asJson(x))
          case _       => resultITX.append(Nil)
        }
        resultITX.append(itx.time.getOrElse(Nil))
        resultITX.append(itx.to.getOrElse(Nil))
        resultITX.append(itx.`type`)
        resultITX.append(itx.value.getOrElse(Nil))
        resultITX.append(s"${itx.blockNumber}_${itx.index}_${itx.callId}")

        resultITXIndex.append(s"${itx.blockNumber}_${itx.index}_${itx.callId}")
        resultITXIndex.append(itx.from.get)
        resultITXIndex.append(itx.blockNumber.toString)
        resultITXIndex.append(itx.index.toString)
        resultITXIndex.append(itx.callId.toString)

        result1.append(resultITX.mkString(FieldDelim))
        result2.append((itx.from.get, resultITXIndex.mkString(FieldDelim)))

        val to = itx.to.getOrElse("-")
        if (itx.from.get != to && to.length > 2) {
          resultITXIndex2.append(
            s"${itx.blockNumber}_${itx.index}_${itx.callId}")
          resultITXIndex2.append(to)
          resultITXIndex2.append(itx.blockNumber.toString)
          resultITXIndex2.append(itx.index.toString)
          resultITXIndex2.append(itx.callId.toString)
          result2.append(
            (itx.to.getOrElse("-"), resultITXIndex2.mkString(FieldDelim)))
        }
      }

    (result1, result2)
  }

  override def loadDataInfile(dbname: String, sql: String): Unit = {
    withDB(dbname) { c =>
      val pstmt = c.prepareStatement(sql)
      pstmt.execute()
      pstmt.close()
    }
  }

  override def loadDataFromS3Block(s3Path: String): Unit = {
    val localFile = s3PathToTempFile(s3Path)
    val sql =
      s"""LOAD DATA LOCAL INFILE '$localFile'
         |IGNORE INTO TABLE ${BlockRepository.BlockTable}
         |FIELDS TERMINATED BY '$FieldDelim'
         |LINES TERMINATED BY '\n'
         |(@block_score,`extra_data`,`gas_used`,@governance_data,`hash`,`logs_bloom`,`number`,`parent_hash`,
         |@proposer,`receipts_root`,`reward`,`size`,`state_root`,`timestamp`,`timestamp_fos`,`total_block_score`,
         |`transaction_count`,`transactions_root`,`vote_data`,`date`,@base_fee_per_gas)
         |SET
         |`block_score` = NULLIF(@block_score, 'NIL'),
         |`governance_data` = NULLIF(@governance_data, 'NIL'),
         |`proposer` = NULLIF(@proposer, 'NIL'),
         |`base_fee_per_gas` = NULLIF(@base_fee_per_gas, 'NIL')
         |""".stripMargin
    loadDataInfile(BlockRepository.BlockDB, sql)
    deleteTempFile(localFile)
  }

  override def loadDataFromS3Transaction(s3Path: String): Unit = {
    val localFile = s3PathToTempFile(s3Path)
    val sql =
      s"""LOAD DATA LOCAL INFILE '$localFile'
         |IGNORE INTO TABLE ${TransactionRepository.TransactionTable}
         |FIELDS TERMINATED BY '$FieldDelim'
         |LINES TERMINATED BY '\n'
         |(`block_hash`,`block_number`,@code_format,@contract_address,@fee_payer,@fee_payer_signatures,@fee_ratio,
         |`from`,`gas`,`gas_price`,`gas_used`,@human_readable,@input,@key,`logs_bloom`,`nft_transfer_count`,`nonce`,
         |`sender_tx_hash`,`signatures`,`status`,`timestamp`,@to,`token_transfer_count`,`transaction_hash`,
         |`transaction_index`,@tx_error,`type`,`type_int`,@value,@access_list,@chain_id,@max_fee_per_gas,@max_priority_fee_per_gas)
         |SET
         |`code_format` = NULLIF(@code_format, 'NIL'),
         |`contract_address` = NULLIF(@contract_address, 'NIL'),
         |`fee_payer` = NULLIF(@fee_payer, 'NIL'),
         |`fee_payer_signatures` = NULLIF(@fee_payer_signatures, 'NIL'),
         |`fee_ratio` = NULLIF(@fee_ratio, 'NIL'),
         |`human_readable` = NULLIF(@human_readable, 'NIL'),
         |`input` = NULLIF(@input, 'NIL'),
         |`key` = NULLIF(@key, 'NIL'),
         |`to` = NULLIF(@to, 'NIL'),
         |`tx_error` = NULLIF(@tx_error, 'NIL'),
         |`value` = NULLIF(@value, 'NIL'),
         |`access_list` = NULLIF(@access_list, 'NIL'),
         |`chain_id` = NULLIF(@chain_id, 'NIL'),
         |`max_fee_per_gas` = NULLIF(@max_fee_per_gas, 'NIL'),
         |`max_priority_fee_per_gas` = NULLIF(@max_priority_fee_per_gas, 'NIL'),
         |`effective_gas_price` = NULLIF(@effective_gas_price, 'NIL')
         |""".stripMargin

    loadDataInfile(TransactionRepository.TransactionDB, sql)
    deleteTempFile(localFile)
  }

  override def loadDataFromS3EventLog(s3Path: String): Unit = {
    val localFile = s3PathToTempFile(s3Path)
    val sql =
      s"""LOAD DATA LOCAL INFILE '$localFile'
         |IGNORE INTO TABLE ${EventLogRepository.EventLogTable}
         |FIELDS TERMINATED BY '$FieldDelim'
         |LINES TERMINATED BY '\n'
         |(`address`,`block_hash`,`block_number`,`signature`,`data`,`log_index`,`topics`,`transaction_hash`,
         |`transaction_index`,@removed)
         |SET
         |`removed` = NULLIF(@removed, 'NIL')
         |""".stripMargin

    loadDataInfile(EventLogRepository.EventLogDB, sql)
    deleteTempFile(localFile)
  }

  override def loadDataFromS3InternalTransaction(s3Path: String,
                                                 dbName: String): Unit = {
    val localFile = s3PathToTempFile(s3Path)
    val sql =
      s"""LOAD DATA LOCAL INFILE '$localFile'
         |IGNORE INTO TABLE ${InternalTransactionRepository.InternalTXTable}
         |FIELDS TERMINATED BY '$FieldDelim'
         |LINES TERMINATED BY '\n'
         |(`block_number`,`call_id`,@error,@from,`gas`,@gas_used,`transaction_index`,
         |@input,@output,@parent_call_id,@reverted,@time,@to,`type`,@value,`internal_tx_id`)
         |SET
         |`error` = NULLIF(@error, 'NIL'),
         |`from` = NULLIF(@from, 'NIL'),
         |`gas_used` = NULLIF(@gas_used, 'NIL'),
         |`input` = NULLIF(@input, 'NIL'),
         |`output` = NULLIF(@output, 'NIL'),
         |`parent_call_id` = NULLIF(@parent_call_id, 'NIL'),
         |`reverted` = NULLIF(@reverted, 'NIL'),
         |`time` = NULLIF(@time, 'NIL'),
         |`to` = NULLIF(@to, 'NIL'),
         |`value` = NULLIF(@value, 'NIL')
         |""".stripMargin

    loadDataInfile(dbName, sql)
    deleteTempFile(localFile)
  }

  override def loadDataFromS3InternalTransactionIndex(s3Path: String,
                                                      dbName: String): Unit = {
    val localFile = s3PathToTempFile(s3Path)
    val sql =
      s"""LOAD DATA LOCAL INFILE '$localFile'
         |IGNORE INTO TABLE  ${InternalTransactionRepository.InternalTXIndexTable}
         |FIELDS TERMINATED BY '$FieldDelim'
         |LINES TERMINATED BY '\n'
         |(`internal_tx_id`,`account_address`,`block_number`,`transaction_index`,`call_id`)
         |""".stripMargin

    loadDataInfile(dbName, sql)
    deleteTempFile(localFile)
  }
}
