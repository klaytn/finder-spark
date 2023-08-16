package io.klaytn.apps.dump

import io.klaytn.dsl.db.withDB
import io.klaytn.utils.spark.{SparkHelper, UserConfig}
import org.apache.spark.sql.SaveMode

import scala.collection.mutable

case class EventSignature(idOf4Byte: Long,
                          hexSignature: String,
                          textSignature: String,
                          primary: Boolean)
case class FunctionSignature(idOf4Byte: Long,
                             bytesSignature: String,
                             textSignature: String,
                             primary: Boolean)

object DumpDBSignatures extends SparkHelper {
  def event(): Unit = {
    val result = mutable.ArrayBuffer.empty[EventSignature]

    withDB("finder0101r") { c =>
      val pstmt =
        c.prepareStatement(
          "SELECT `4byte_id`,`hex_signature`,`text_signature`,`primary` FROM `event_signatures`")

      val rs = pstmt.executeQuery()
      while (rs.next()) {
        result.append(
          EventSignature(rs.getLong(1),
                         rs.getString(2),
                         rs.getString(3),
                         rs.getBoolean(4))
        )
      }

      rs.close()
      pstmt.close()
    }

    import spark.implicits._
    spark
      .createDataset(result)
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(s"${UserConfig.logStorageS3Path}label=event_signatures")
  }

  def function(): Unit = {
    val result = mutable.ArrayBuffer.empty[FunctionSignature]

    withDB("finder0101r") { c =>
      val pstmt =
        c.prepareStatement(
          "SELECT `4byte_id`,`bytes_signature`,`text_signature`,`primary` FROM `function_signatures`")

      val rs = pstmt.executeQuery()
      while (rs.next()) {
        result.append(
          FunctionSignature(rs.getLong(1),
                            rs.getString(2),
                            rs.getString(3),
                            rs.getBoolean(4))
        )
      }

      rs.close()
      pstmt.close()
    }

    import spark.implicits._
    spark
      .createDataset(result)
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(s"${UserConfig.logStorageS3Path}label=function_signatures")
  }

  override def run(args: Array[String]): Unit = {
    function()
    event()
  }
}
