package io.klaytn.tools.monitoring

import io.klaytn.tools.model.ServiceType
import io.klaytn.tools.reporter.SlackReporter
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.exception.ExceptionUtils

import java.text.SimpleDateFormat
import java.util.TimeZone

trait MonitoringService {
  val reporter: SlackReporter = SlackReporter.error()

  val sdf: SimpleDateFormat = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    sdf.setTimeZone(TimeZone.getTimeZone("Asia/Seoul"))
    sdf
  }

  def isKlaytn(phase: String): Boolean =
    Set("cypress", "baobab").contains(phase)

  def sendErrorMsg(msg: String): Unit = reporter.report(msg)

  protected def monitoring(typ: ServiceType.Value): Unit

  def run(typ: ServiceType.Value): Unit = {
    try {
      monitoring(typ)
    } catch {
      case e: Throwable =>
        reporter.report(s"""${this.getClass.getSimpleName.stripSuffix("$")}
                           | running monitor failed
                           | exception: ${e.getLocalizedMessage}
                           | stacktrace: ${StringUtils.abbreviate(
                             ExceptionUtils.getStackTrace(e),
                             800)}""".stripMargin)
    }
  }
}
