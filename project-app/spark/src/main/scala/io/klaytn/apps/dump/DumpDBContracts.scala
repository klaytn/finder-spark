package io.klaytn.apps.dump

import io.klaytn.client.FinderRedis
import io.klaytn.dsl.db.withDB
import io.klaytn.utils.spark.{SparkHelper, UserConfig}
import org.apache.spark.sql.SaveMode

import scala.collection.mutable

case class Contracts(contractAddress: String,
                     contractType: Int,
                     name: String,
                     symbol: String,
                     decimal: Int,
                     implementationAddress: String,
                     verified: Boolean)

object DumpDBContracts extends SparkHelper {
  override def run(args: Array[String]): Unit = {
    val result = mutable.ArrayBuffer.empty[Contracts]

    // TODO: Manage identities to incrementally re-dump the whole thing every time, since it's currently small.
    val lastId = FinderRedis.get("DumpContractDB:LastId").getOrElse("0").toLong
    withDB("finder0101r") { c =>
      val pstmt = c.prepareStatement(
        "SELECT `id`, `contract_address`, `contract_type`, `name`, `symbol`, `decimal`, `implementation_address`, `verified` FROM `contracts` WHERE id > ?")
      pstmt.setLong(1, lastId)

      val rs = pstmt.executeQuery()
      while (rs.next()) {
        result.append(
          Contracts(rs.getString(2),
                    rs.getInt(3),
                    rs.getString(4),
                    rs.getString(5),
                    rs.getInt(6),
                    rs.getString(7),
                    rs.getBoolean(8))
        )
      }

      rs.close()
      pstmt.close()
    }

    import spark.implicits._
    spark
      .createDataset(result)
      .repartition(4)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(UserConfig.logStorageS3Path)
  }
}
