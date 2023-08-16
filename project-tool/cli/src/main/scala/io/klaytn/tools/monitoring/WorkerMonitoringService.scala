package io.klaytn.tools.monitoring
import io.klaytn.datasource.redis.FinderRedisClient
import io.klaytn.tools.Utils
import io.klaytn.tools.dsl.db.withDB
import io.klaytn.tools.model.ServiceType
import io.klaytn.tools.reporter.SlackReporter

import java.text.SimpleDateFormat
import java.util.TimeZone

class WorkerMonitoringService extends MonitoringService {

  private def getMaxIdAndTS(dbName: String, tableName: String): (Long, Int) = {
    withDB(dbName) { c =>
      val pstmt = c.prepareStatement(
        s"SELECT `id`,`timestamp` FROM $tableName ORDER BY id DESC LIMIT 1")
      val rs = pstmt.executeQuery()
      val result = if (rs.next()) (rs.getLong(1), rs.getInt(2)) else (0L, 0)
      rs.close()
      pstmt.close()
      result
    }
  }

  private def getTSById(dbName: String, tableName: String, id: Long): Int = {
    withDB(dbName) { c =>
      val pstmt =
        c.prepareStatement(s"SELECT `timestamp` FROM $tableName WHERE `id`=?")
      pstmt.setLong(1, id)
      val rs = pstmt.executeQuery()
      val result = if (rs.next()) rs.getInt(1) else 0
      rs.close()
      pstmt.close()
      result
    }
  }

  private def errorMsg(phase: String,
                       msgKey: String,
                       ts1: Int,
                       ts2: Int,
                       id1: Long,
                       id2: Long): Unit = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    sdf.setTimeZone(TimeZone.getTimeZone("Asia/Seoul"))

    val reporter = SlackReporter.error()
    val msg = s"""
                 |⚠️ [$phase] $msgKey Not Updated.
                 | Checked Last updated: ${sdf.format(ts1 * 1000L)} / $id1
                 | DB Last updated: ${sdf.format(ts2 * 1000L)} / $id2
                 |""".stripMargin

    reporter.report(msg)
  }

  private def proc(phase: String,
                   dbName: String,
                   tableName: String,
                   redis: FinderRedisClient,
                   redisKey: String): Unit = {
    val redisMaxId = redis.get(redisKey).getOrElse("0").toLong
    val ts = getTSById(dbName, tableName, redisMaxId)
    val diff = (System.currentTimeMillis() / 1000).toInt - ts
    println(s"- [$phase] redis max id: $redisMaxId ; ts: $ts ; diff: $diff")
    // Notify when the ts of the MAX ID value in REDIS differs from the current time by more than 1 minute, and the DB recent ID and the ID in REDIS are different.
    if (diff > 60) {
      val (dbMaxId, dbMaxTS) = getMaxIdAndTS(dbName, tableName)
      Thread.sleep(5000) // sleep for 5 seconds, check data
      val redisMaxId = redis.get(redisKey).getOrElse("0").toLong
      if (redisMaxId < dbMaxId) {
        println(s"- [$phase] error: db max id: $dbMaxId; ts: $ts")
        errorMsg(phase, redisKey, ts, dbMaxTS, redisMaxId, dbMaxId)
      }
    }
  }

  private def token(phase: String, redis: FinderRedisClient): Unit = {
    val dbName = if (phase == "cypress") "finder03r" else phase + "r"
    val tableName =
      if (isKlaytn(phase)) "token_transfers" else "token_transfers"

    println(s"\n[$phase] start token...")
    proc(phase, dbName, tableName, redis, "HolderService:Token:LastBlock")
  }

  private def tokenBurn(phase: String, redis: FinderRedisClient): Unit = {
    val dbName = if (phase == "cypress") "finder03r" else phase + "r"
    val tableName = "token_burns"

    println(s"\n[$phase] start token burn...")
    proc(phase, dbName, tableName, redis, "HolderService:TokenBurn:LastBlock")
  }

  private def nft(phase: String, redis: FinderRedisClient): Unit = {
    val dbName = if (phase == "cypress") "finder03r" else phase + "r"
    val tableName =
      if (isKlaytn(phase)) "nft_transfers" else "nft_transfers"

    println(s"\n[$phase] start nft...")
    proc(phase, dbName, tableName, redis, "HolderService:NFT:LastBlock")
  }

  private def burntByGasFee(phase: String): Unit = {
    val dbName = if (phase == "cypress") "finder0101r" else phase + "r"

    println(s"\n[$phase] start burnt by gas fee...")
    val blockBurnInfo = withDB(dbName) { c =>
      val pstmt = c.prepareStatement(
        "SELECT `number`,`timestamp` FROM `block_burns` ORDER BY `number` DESC LIMIT 1")
      val rs = pstmt.executeQuery()

      val result = if (rs.next()) (rs.getLong(1), rs.getInt(2)) else (0L, 0)

      rs.close()
      pstmt.close()

      result
    }

    val blockInfo = withDB(dbName) { c =>
      val pstmt = c.prepareStatement(
        "SELECT `number`,`timestamp` FROM `blocks` ORDER BY `number` DESC LIMIT 1")
      val rs = pstmt.executeQuery()

      val result = if (rs.next()) (rs.getLong(1), rs.getInt(2)) else (0L, 0)

      rs.close()
      pstmt.close()

      result
    }

    println(
      s"- [$phase] block burn info: ${blockBurnInfo._1} / block info: ${blockInfo._1} / diff: ${blockInfo._1 - blockBurnInfo._1}")

    // Error if burns is not renewed for more than 60 seconds
    if (blockInfo._1 - blockBurnInfo._1 > 60) {
      errorMsg(phase,
               "BurntByGasFee",
               blockBurnInfo._2,
               blockInfo._2,
               blockBurnInfo._1,
               blockInfo._1)
    }
  }

  override def monitoring(typ: ServiceType.Value): Unit = {
    Utils.getPhaseAndRedisClients(typ) foreach {
      case (phase, redis) =>
        token(phase, redis)
        tokenBurn(phase, redis)

        if (typ == ServiceType.klaytn) {
          burntByGasFee(phase)
          nft(phase, redis)
        }
    }
  }
}
