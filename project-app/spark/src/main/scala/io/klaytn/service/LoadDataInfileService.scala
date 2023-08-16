package io.klaytn.service

import io.klaytn.model._
import io.klaytn.service.load.DefaultLoadDataInfileService

object LoadDataInfileService {
  def of(chainPhase: ChainPhase): LoadDataInfileService = {
    chainPhase.chain match {
      case Chain.baobab | Chain.cypress =>
        new DefaultLoadDataInfileService()
    }
  }
}

trait LoadDataInfileService {
  def writeLoadData(jobBasePath: String,
                    typ: String,
                    blockNumber: Long,
                    content: Seq[String]): Option[String]
  def loadDataAndDeleteFile(savePath: String, dbName: Option[String]): Unit
  def blockLine(block: RefinedBlock): String
  def transactionReceiptLines(
      transactionReceipts: List[RefinedTransactionReceipt]): Seq[String]
  def transactionReceiptLines(block: Block): Seq[String]
  def eventLogLines(eventLogs: List[RefinedEventLog]): Seq[String]
  def internalTransactionLines(itxs: List[RefinedInternalTransactions])
    : (Seq[String], Seq[(String, String)])
  def loadDataInfile(dbname: String, sql: String): Unit
  def loadDataFromS3Block(s3Path: String): Unit
  def loadDataFromS3Transaction(s3Path: String): Unit
  def loadDataFromS3EventLog(s3Path: String): Unit
  def loadDataFromS3InternalTransaction(s3Path: String, dbName: String): Unit
  def loadDataFromS3InternalTransactionIndex(s3Path: String,
                                             dbName: String): Unit
}
