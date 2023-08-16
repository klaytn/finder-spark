package io.klaytn.apps.batch

import io.klaytn.dsl.db.{getDataSource, withDB}
import io.klaytn.utils.config.Cfg
import io.klaytn.utils.spark.SparkHelper
import io.klaytn.utils.{DateUtil, Utils}
import org.joda.time.DateTime

import java.sql.Timestamp
import scala.collection.mutable

trait BatchHelper extends SparkHelper with Serializable {
  val DatePattern = "yyyyMMdd"

  var today: DateTime = DateUtil.todayUTC().minusDays(1)

  def todayYMD: String =
    DateUtil.toUTCString(new Timestamp(today.getMillis), DatePattern)

  def isMonday: Boolean = today.dayOfWeek().get() == 1

  def isFirstDay: Boolean = today.dayOfMonth().get() == 1

  def toYMD(ts: Int): String =
    DateUtil.toUTCString(new Timestamp(ts * 1000L), DatePattern)

  def daily(ymd: String, bnps: Set[String]): Unit

  def weekly(startYMD: String, endYMD: String, bnps: Set[String]): Unit

  def monthly(startYMD: String, endYMD: String, bnps: Set[String]): Unit

  private def checkAndSetBlockDateMap(): Unit = {
    var dbYMD: Int = 0
    var dbMaxBlockNumber: Long = 0L
    withDB(getDataSource("statr")) { c =>
      val pstmt =
        c.prepareStatement(
          "SELECT `ymd`, `max_block_number` FROM `block_date_maps` ORDER BY `ymd` DESC LIMIT 1")
      val rs = pstmt.executeQuery()
      if (rs.next()) {
        dbYMD = rs.getInt(1)
        dbMaxBlockNumber = rs.getLong(2)
      }
      rs.close()
      pstmt.close()
    }

    if (dbYMD >= todayYMD.toInt) return

    val blockNumberTsDataList = mutable.ArrayBuffer.empty[(Long, Int)]

    withDB(getDataSource("finder0101r")) { c =>
      val pstmt = c.prepareStatement(
        "SELECT `number`, `timestamp` FROM `blocks` WHERE `number` > ?")
      pstmt.setLong(1, dbMaxBlockNumber)
      val rs = pstmt.executeQuery()
      while (rs.next()) {
        blockNumberTsDataList.append((rs.getLong(1), rs.getInt(2)))
      }

      rs.close()
      pstmt.close()
    }

    val result = mutable.Map.empty[Int, (Long, Int, Long, Int)]

    blockNumberTsDataList.foreach {
      case (blockNumber, blockTs) =>
        val ymd =
          DateUtil.toUTCString(new Timestamp(blockTs * 1000L), "yyyyMMdd").toInt

        val v =
          result.getOrElseUpdate(ymd,
                                 (blockNumber, blockTs, blockNumber, blockTs))
        val newV =
          if (v._1 > blockNumber) (blockNumber, blockTs, v._3, v._4)
          else if (v._3 < blockNumber) (v._1, v._2, blockNumber, blockTs)
          else v

        result(ymd) = newV
    }

    (dbYMD + 1) to todayYMD.toInt foreach { ymd =>
      val (minBlockNumber, minBlockTs, maxBlockNumber, maxBlockTs) = result(ymd)
      withDB(getDataSource("stat")) { c =>
        val pstmt = c.prepareStatement(
          "INSERT IGNORE INTO `block_date_maps` (`ymd`, `min_block_number`, `min_block_timestamp`, `max_block_number`, `max_block_timestamp`) values (?,?,?,?,?)")

        pstmt.setInt(1, ymd)
        pstmt.setLong(2, minBlockNumber)
        pstmt.setInt(3, minBlockTs)
        pstmt.setLong(4, maxBlockNumber)
        pstmt.setInt(5, maxBlockTs)

        pstmt.execute()
        pstmt.close()
      }
    }
  }

  private def getBNPs(startYMD: String, endYMD: String): Set[String] = {
    val bnps = mutable.Set.empty[String]

    withDB(getDataSource("statr")) { c =>
      val pstmt =
        c.prepareStatement(
          "SELECT `min_block_number`, `max_block_number` FROM `block_date_maps` WHERE `ymd`>=? and `ymd`<=?")
      pstmt.setInt(1, startYMD.toInt)
      pstmt.setInt(2, endYMD.toInt)
      val rs = pstmt.executeQuery()
      while (rs.next()) {
        bnps.add(Utils.getBlockNumberPartition(rs.getLong(1)))
        bnps.add(Utils.getBlockNumberPartition(rs.getLong(2)))
      }

      rs.close()
      pstmt.close()
    }

    bnps.toSet
  }

  override def run(args: Array[String]): Unit = {
    if (Cfg.hasPath("spark.app.todayYMD"))
      today = DateUtil.parse(Cfg.getString("spark.app.todayYMD"))

//    val startDate = DateUtil.parse("20190624")
//    0 to 1218 foreach { d =>
    val startDate = DateUtil.parse("20190626")
    0 to 1216 foreach { d =>
      today = startDate.plusDays(d)

      checkAndSetBlockDateMap()

      daily(todayYMD, getBNPs(todayYMD, todayYMD))

      if (isMonday) {
        val startYMD = DateUtil.toUTCString(today.minusDays(7), DatePattern)
        val endYMD = DateUtil.toUTCString(today.minusDays(1), DatePattern)
        weekly(startYMD, endYMD, getBNPs(startYMD, endYMD))
      }

      if (isFirstDay) {
        val startYMD = DateUtil.toUTCString(today.minusDays(1), "yyyyMM01")
        val endYMD = DateUtil.toUTCString(today.minusDays(1), DatePattern)
        monthly(startYMD, endYMD, getBNPs(startYMD, endYMD))
      }
    }
  }
}
