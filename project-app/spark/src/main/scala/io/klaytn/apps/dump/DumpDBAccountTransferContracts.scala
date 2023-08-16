package io.klaytn.apps.dump

import io.klaytn.dsl.db.withDB
import io.klaytn.utils.spark.{SparkHelper, UserConfig}
import org.apache.spark.sql.SaveMode

import scala.collection.mutable

case class AccountTransferContracts(accountAddress: String,
                                    contractAddress: String,
                                    transferType: Int,
                                    accountPrefix: String)

object DumpDBAccountTransferContracts extends SparkHelper {
  override def run(args: Array[String]): Unit = {
    val totalCount = withDB("finder0101r") { c =>
      val pstmt =
        c.prepareStatement("SELECT COUNT(*) FROM `account_transfer_contracts`")
      val rs = pstmt.executeQuery()
      val count = if (rs.next()) rs.getLong(1) else 0L
      rs.close()
      pstmt.close()
      count
    }

    val workerCount = 16
    val perCount = totalCount / workerCount

    val range = 0 until workerCount map { idx =>
      val min = idx * perCount
      val max =
        if (idx + 1 == workerCount) totalCount * 2 else (idx + 1) * perCount
      (idx, (min, max))
    }

    val result = sc.parallelize(range).flatMap {
      case (_, (min, max)) =>
        val result = mutable.ArrayBuffer.empty[AccountTransferContracts]
        withDB("finder0101r") { c =>
          val pstmt = c.prepareStatement(
            "SELECT `account_address`, `contract_address`, `transfer_type` FROM `account_transfer_contracts` WHERE id >= ? AND id < ?")

          pstmt.setLong(1, min)
          pstmt.setLong(2, max)

          val rs = pstmt.executeQuery()
          while (rs.next()) {
            result.append(
              AccountTransferContracts(rs.getString(1),
                                       rs.getString(2),
                                       rs.getInt(3),
                                       rs.getString(1).substring(0, 3))
            )
          }

          rs.close()
          pstmt.close()
        }
        result
    }

    import spark.implicits._
    spark
      .createDataset(result)
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("accountPrefix")
      .parquet(UserConfig.logStorageS3Path)
  }
}
