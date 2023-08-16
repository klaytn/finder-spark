package io.klaytn.tools.monitoring
import io.klaytn.tools.Utils
import io.klaytn.tools.dsl.db.withDB
import io.klaytn.tools.model.ServiceType

import java.io.{BufferedWriter, File, FileWriter}
import scala.io.Source

class BlockMonitoringService extends MonitoringService {
  def getMaxTs(phase: String): Option[(String, Long)] = {
    val dbName = if (phase == "cypress") "finder0101r" else phase + "r"

    withDB(dbName) { c =>
      val pstmt =
        c.prepareStatement(
          "SELECT MAX(`timestamp`) FROM (SELECT `timestamp` FROM `blocks` ORDER BY `number` DESC LIMIT 10) a")

      val rs = pstmt.executeQuery()
      val result =
        if (rs.next()) Some((phase, rs.getLong(1)))
        else None

      rs.close()
      pstmt.close()

      result
    }
  }

  override def monitoring(typ: ServiceType.Value): Unit = {
    val phases = Utils.getPhaseAndRedisClients(typ).map(_._1)

    phases.flatMap(phase => getMaxTs(phase)).foreach {
      case (phase, timestamp) =>
        val errorFile = s"/home/ssm-user/block_monitoring_error.$phase.txt"
        val existErrorFile = new File(errorFile).exists()

        val lastBlockTs = timestamp * 1000
        if (lastBlockTs < System
              .currentTimeMillis() - 10000) { // If the block is not updated for more than 10 seconds...
          val (count, beforeNotiTs) = if (existErrorFile) {
            val src = Source.fromFile(errorFile)
            val s = src.getLines().mkString.split("\t")
            src.close()
            (s.head.toInt, s.last.toLong)
          } else (0, 0L)

          // Let the time increment based on the number of notices, but not more than 10 minutes.
          val notiTerm = if (count < 10) count * 60 * 1000 else 10 * 60 * 1000
          if (beforeNotiTs + notiTerm < System.currentTimeMillis()) {
            val msg =
              s"""
                 |🚨 [$phase] The block is not updated.
                 | Last updated: ${sdf.format(lastBlockTs)}
                 | block restore stop(redis): setex spark:app:prod-$phase:FastWorkerStreaming:DoNotRestoreBlock 10000 run
                 | force block restore (redis): setex spark:app:prod-$phase:FastWorkerStreaming:ForceRestoreBlock 10000 run
                 |""".stripMargin

            reporter.report(msg)

            val bw = new BufferedWriter(new FileWriter(errorFile))
            bw.write(s"${count + 1}\t${System.currentTimeMillis()}\n")
            bw.close()
          }
        } else {
          if (existErrorFile) new File(errorFile).delete()

          println(
            s"[$phase] success. lastBlockTs: $lastBlockTs ; current: ${System
              .currentTimeMillis()} ; diff ${System.currentTimeMillis() - lastBlockTs}")
        }
    }
  }
}
