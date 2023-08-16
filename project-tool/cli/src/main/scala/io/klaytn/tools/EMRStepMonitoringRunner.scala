package io.klaytn.tools

import io.klaytn.tools.monitoring.EMRStepMonitoringService

import scala.util.Try

object EMRStepMonitoringRunner {
  def main(args: Array[String]): Unit = {
    new EMRStepMonitoringService(Try { args(1) }.toOption)
      .run(Utils.getType(args))
  }
}
