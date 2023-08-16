package io.klaytn.tools

import io.klaytn.tools.monitoring.BlockMonitoringService

object BlockMonitoringRunner {
  def main(args: Array[String]): Unit = {
    new BlockMonitoringService().run(Utils.getType(args))
  }
}
