package io.klaytn.tools.monitoring
import io.klaytn.tools.Utils
import io.klaytn.tools.client.EMRClient
import io.klaytn.tools.dsl.db.withDB
import io.klaytn.tools.model.ServiceType
import io.klaytn.tools.model.monitor.MonitoringRow

import scala.collection.mutable

class EMRStepMonitoringService(optProfile: Option[String])
    extends MonitoringService {
  private def getMonitoringRows(
      phases: Seq[String]): Map[String, MonitoringRow] = {
    val monitoringTargetByName = mutable.Map.empty[String, MonitoringRow]

    phases.foreach { phase =>
      val dbName = if (phase == "cypress") "finder0101r" else phase + "r"

      withDB(dbName) { c =>
        val pstmt = c.prepareStatement(
          "select `step_name`,`auto_recovery` from `emr_spark_job_monitoring_list`")

        val rs = pstmt.executeQuery()
        while (rs.next()) {
          monitoringTargetByName(rs.getString(1)) =
            MonitoringRow(rs.getString(1), rs.getBoolean(2))
        }

        rs.close()
        pstmt.close()
      }
    }

    monitoringTargetByName.toMap
  }

  private def getOptInvalidStatusStepMessage(
      invalidStatusSteps: Seq[String],
      monitoringTargetByName: Map[String, MonitoringRow]): Option[String] = {
    if (invalidStatusSteps.isEmpty) None
    else {
      val stepMsg = invalidStatusSteps
        .map(name => monitoringTargetByName(name))
        .map(sheetRow =>
          s"${sheetRow.stepName}${if (sheetRow.autoRecovery) " - auto recovery"
          else ""}")
        .mkString("\n")
      val message = s"""[An unhealthy STEP]
                       |$stepMsg
                       |""".stripMargin
      Some(message)
    }
  }

  private def getPhaseStrings(
      invalidStatusSteps: Seq[String],
      monitoringTargetByName: Map[String, MonitoringRow]): String = {
    invalidStatusSteps
      .map(name => monitoringTargetByName(name))
      .map(target => target.phaseString)
      .groupBy(x => x)
      .mapValues(_.size)
      .map { case (phaseString, count) => s"$phaseString:$count" }
      .mkString(",")
  }

  override def monitoring(typ: ServiceType.Value): Unit = {
    val phases = Utils.getPhaseAndRedisClients(typ).map(_._1)

    val monitoringTargetByName = getMonitoringRows(phases)
    val monitoringStepNames = monitoringTargetByName.keys.toSeq

    println("DB Data...")
    monitoringTargetByName.foreach(m =>
      println(s"${m._1}\t${m._2.stepName}\t${m._2.autoRecovery}"))

    val client = new EMRClient(optProfile)
    val clusters = client.getRunningClusters()

    val stepByName =
      clusters.flatMap(cluster => cluster.steps.map(x => (x.name, x))).toMap
    val invalidStatusSteps = monitoringStepNames.diff(stepByName.keys.toSeq)

    println("\nEMR Data...")
    stepByName.foreach(m =>
      println(
        s"${m._1}\t${m._2.name}\t${m._2.id}\t${m._2.state}\t${m._2.clusterId}"))

    // todo
//    val unknownSteps = stepByName.keys.toSeq.diff(monitoringStepNames)

    getOptInvalidStatusStepMessage(invalidStatusSteps, monitoringTargetByName) match {
      case Some(msg) =>
        val phaseStrings =
          getPhaseStrings(invalidStatusSteps, monitoringTargetByName)
        val message = s"""❌ EMR Cluster Monitoring ($phaseStrings) ❌
                         |cluster: $typ
                         |aws_profile: ${optProfile.getOrElse("none")}
                         |
                         |${msg}""".stripMargin
        reporter.report(message)
      case _ =>
    }

    invalidStatusSteps
      .map(monitoringTargetByName(_))
      .filter(target => target.autoRecovery)
      .par
      .foreach { target =>
        val msg = s"""Start auto recovery
                     |cluster: $typ
                     |aws_profile: ${optProfile.getOrElse("none")}
                     | > ${target.stepName}
                     |""".stripMargin
        reporter.report(msg)
        val (result, recoveryMsg) =
          client.recoveryStep(target.stepName, ignoreRunningState = false)
        reporter.report(s"""Auto Recover ${if (result) "Success" else "Failed."}
                           |cluster: $typ
                           |aws_profile: ${optProfile.getOrElse("none")}
                           | > ${target.stepName}
                           |$recoveryMsg
                           |""".stripMargin)
      }
  }
}
