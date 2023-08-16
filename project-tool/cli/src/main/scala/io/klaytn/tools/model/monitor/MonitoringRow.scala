package io.klaytn.tools.model.monitor

case class MonitoringRow(stepName: String, autoRecovery: Boolean) {
  def phaseString =
    if (isDev) {
      "dev"
    } else if (isProd) {
      "prod"
    } else {
      "unknown"
    }
  private def isDev: Boolean = {
    stepName.contains("-dev-")
  }

  private def isProd: Boolean = {
    stepName.contains("-prod-")
  }
}
