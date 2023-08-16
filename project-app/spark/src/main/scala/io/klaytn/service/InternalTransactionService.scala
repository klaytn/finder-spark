package io.klaytn.service

import io.klaytn._
import io.klaytn.model.RefinedInternalTransactions
import io.klaytn.persistent.InternalTransactionPersistentAPI
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

class InternalTransactionService(
    internalTransactionPersistentAPI: LazyEval[
      InternalTransactionPersistentAPI])
    extends Serializable {
  def saveInternalTransactionToMysql(
      internalTransactions: List[RefinedInternalTransactions]): Unit = {
    try {
      internalTransactionPersistentAPI.insert(internalTransactions)
    } catch {
      case _: Throwable =>
        internalTransactions.foreach { r =>
          try {
            internalTransactionPersistentAPI.insert(List(r))
          } catch {
            case _: Throwable =>
          }
        }
    }
  }

  def saveInternalTransactionIndexToMysql(
      internalTransactions: List[RefinedInternalTransactions]): Unit = {
    try {
      internalTransactionPersistentAPI.insertIndex(internalTransactions)
    } catch {
      case e: Throwable =>
        internalTransactions.foreach { r =>
          try {
            internalTransactionPersistentAPI.insertIndex(List(r))
          } catch {
            case _: Throwable =>
          }
        }
    }
  }

  def saveInternalTransactionToS3(
      data: RDD[RefinedInternalTransactions],
      saveDir: String,
      numPartitions: Int)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    val internalTransactionDS = spark.createDataset(data)

    internalTransactionDS
      .repartition(numPartitions)
      .write
      .mode(SaveMode.Append)
      .partitionBy("label", "bnp")
      .parquet(saveDir)
  }
}
