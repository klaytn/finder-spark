package io.klaytn.apps.block

import io.klaytn.utils.SlackUtil
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.exception.ExceptionUtils

class BlockToESErrorReporter extends Serializable {
  def report(e: Throwable): Unit = {
    val message = s"""class: BlockToESStreaming
                     |errorMessage: ${e.getMessage}
                     |stackTrace: ${StringUtils.abbreviate(
                       ExceptionUtils.getStackTrace(e),
                       800)}
                     |""".stripMargin
    SlackUtil.sendMessage(message)
  }
}
