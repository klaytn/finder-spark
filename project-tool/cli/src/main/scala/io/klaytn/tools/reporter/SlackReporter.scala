package io.klaytn.tools.reporter

object SlackReporter {
  def error(): SlackReporter = {
    val errorWebHookUrl = ""
    new SlackReporter(errorWebHookUrl)
  }
}

class SlackReporter(webHookUrl: String) extends Serializable {
  private val fixedHeader = Map("Content-Type" -> "application/json")

  def report(input: String): Unit = {
    val message = input.replace("\"", "'")
    val requestBody = s"""{"text":"```$message```"}"""
    try {
      val response = requests.post(url = webHookUrl,
                                   data = requestBody,
                                   headers = fixedHeader)
      if (!response.is2xx) {
        sendFailedMessage(message)
      }
    } catch {
      case e: Throwable =>
        val failedMessage =
          s"""Fail to send slack message
             |error: ${e.getLocalizedMessage}
             |size: ${message.length}
             |message(200): ${message.take(200)}
             |""".stripMargin
        sendFailedMessage(failedMessage)
    }
  }

  private def sendFailedMessage(message: String): Unit = {
    val requestBody = s"""{"text":"```$message```"}"""
    try {
      val response = requests.post(url = webHookUrl,
                                   data = requestBody,
                                   headers = fixedHeader)
      if (!response.is2xx) {
        throw new IllegalStateException(response.text())
      }
    } catch {
      case e: Throwable =>
        System.err.println(s"""Send Slack Failed
                              |slack_error: ${e.getLocalizedMessage}
                              |message: $message
                              |""".stripMargin)
        e.printStackTrace()
    }
  }
}
