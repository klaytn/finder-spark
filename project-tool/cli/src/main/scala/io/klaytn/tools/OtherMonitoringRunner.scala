package io.klaytn.tools

import io.klaytn.tools.monitoring.{MonitoringService, WorkerMonitoringService}

object OtherMonitoringRunner {
  def main(args: Array[String]): Unit = {
    val jobs: Seq[MonitoringService] = Seq(new WorkerMonitoringService())

    val typ = Utils.getType(args)
    jobs.foreach(job => job.run(typ))
    System.exit(0)
  }
}
