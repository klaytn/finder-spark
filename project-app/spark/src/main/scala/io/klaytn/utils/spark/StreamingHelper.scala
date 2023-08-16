package io.klaytn.utils.spark

import io.klaytn.utils.SlackUtil
import io.klaytn.utils.s3.S3Util
import org.apache.spark.streaming.{Duration, StreamingContext}

trait StreamingHelper extends SparkHelper {
  def batchDuration(): Duration = UserConfig.batchDuration
  def stopFlagKey(): String =
    s"jobs/$appName/if-want-stop-delete-this-file.${UserConfig.chainPhase}.txt"
  def kafkaLogBufferKey(): String = s"jobs/$appName/kafkaLogBuffer"

  lazy val ssc: StreamingContext =
    new StreamingContext(sc, this.batchDuration())

  private def shutdownMonitor(): Unit = {
    val content = s"AppID: ${sc.applicationId}"
    S3Util.writeText(UserConfig.baseBucket, stopFlagKey(), content)

    val monitorIntervalMs = 5000
    var stop: Boolean = false

    while (!stop) {
      stop = ssc.awaitTerminationOrTimeout(monitorIntervalMs)
      if (stop) {
        SlackUtil.sendMessage(s"[$jobName] The streaming context is stopped...")
      }

      if (!stop && !S3Util.exist(UserConfig.baseBucket, stopFlagKey())) {
        SlackUtil.sendMessage(s"[$jobName] Start to stop gracefully...")
        ssc.stop(true, true)
        SlackUtil.sendMessage(s"[$jobName] Success to stop gracefully...")
      }
    }
  }

  override def startMessage(): String = {
    s"""${super.startMessage()}
       |duration: ${batchDuration()}
       |consume mode: ${UserConfig.consumeMode}
       |streaming monitoring max deploy: ${UserConfig.streamingMonitoringMaxDeploy}
       |streaming monitoring notify interval: ${UserConfig.streamingMonitoringNotifyInterval}
       |""".stripMargin
  }

  override def main(args: Array[String]): Unit = {
    beforeJobStart()

    run(args)

    ssc.start()

    SparkStreamingMonitor.run(ssc, appName)
    shutdownMonitor()

    afterJobFinish()
  }
}
