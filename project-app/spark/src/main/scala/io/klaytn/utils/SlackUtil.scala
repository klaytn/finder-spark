package io.klaytn.utils

import io.klaytn.model.ChainPhase
import io.klaytn.utils.http.HttpClient
import io.klaytn.utils.spark.UserConfig

object SlackUtil {
  private lazy val httpClient = HttpClient.createPooled()
  private val webHookUrl = UserConfig.webHookUrl
  private val fixedHeader = Map("Content-Type" -> "application/json")

  def sendMessage(input: String): Unit = {
    val message = s"${UserConfig.chainPhase}\n$input".replace("\"", "'")
    val requestBody = s"""{"text":"```$message```"}"""
    try {
      val response = httpClient.post(url = webHookUrl,
                                     data = requestBody,
                                     headers = fixedHeader)
      if (!response.is2xx) {
        sendFailedMessage(message)
      }
    } catch {
      case e: Throwable =>
      // val failedMessage =
      //   s"""Fail to send slack message
      //      |error: ${e.getLocalizedMessage}
      //      |size: ${message.length}
      //      |message(200): ${message.take(200)}
      //      |""".stripMargin
      // sendFailedMessage(failedMessage)
    }
  }

  private def sendFailedMessage(message: String): Unit = {
    val requestBody = s"""{"text":"```$message```"}"""
    try {
      val response = httpClient.post(url = webHookUrl,
                                     data = requestBody,
                                     headers = fixedHeader)
      if (!response.is2xx) {
        throw new IllegalStateException(response.body)
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
