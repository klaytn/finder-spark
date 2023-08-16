package io.klaytn.utils.spark

import io.klaytn.model.ChainPhase
import io.klaytn.utils.{SlackUtil, TimeUtil}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.scheduler._

class SparkStreamingMonitor(appName: String,
                            chainPhase: ChainPhase,
                            maxProcessingDelay: Long,
                            notifyInterval: Long = 60000)
    extends StreamingListener
    with TimeUtil {
  var latestSendTime: Long = 0L

  def sendDelayMessage(totalDelay: Long,
                       processingDelay: Long,
                       schedulingDelay: Long,
                       numRecords: Long): Unit = {
    val msg =
      s"""[Spark Streaming Monitor]
         |Job Name : $appName-$chainPhase
         |Expected max delay : ${readableTime(maxProcessingDelay)}
         |Batch records : $numRecords
         |Total delay: ${readableTime(totalDelay)}
         |Processing delay: ${readableTime(processingDelay)}
         |Scheduling delay: ${readableTime(schedulingDelay)}
         |""".stripMargin
//    println(msg)
    SlackUtil.sendMessage(msg)
  }

  override def onBatchCompleted(
      batchCompleted: StreamingListenerBatchCompleted): Unit = {
    val batchInfo = batchCompleted.batchInfo
    val totalDelay = batchInfo.totalDelay.getOrElse(0L)
    val processingDelay = batchInfo.processingDelay.getOrElse(0L)
    val schedulingDelay = batchInfo.schedulingDelay.getOrElse(0L)
    val numRecords = batchInfo.numRecords

    this.synchronized {
      val now = System.currentTimeMillis()
      if (totalDelay >= maxProcessingDelay) {
        if (now - latestSendTime >= notifyInterval) {
          sendDelayMessage(totalDelay,
                           processingDelay,
                           schedulingDelay,
                           numRecords)
          latestSendTime = now
        }
      }
    }
  }

  override def onReceiverStarted(
      receiverStarted: StreamingListenerReceiverStarted): Unit = {}
  override def onReceiverError(
      receiverError: StreamingListenerReceiverError): Unit = {}
  override def onReceiverStopped(
      receiverStopped: StreamingListenerReceiverStopped): Unit = {}
  override def onBatchSubmitted(
      batchSubmitted: StreamingListenerBatchSubmitted): Unit = {}
  override def onBatchStarted(
      batchStarted: StreamingListenerBatchStarted): Unit = {}
}

object SparkStreamingMonitor {
  def run(ssc: StreamingContext, appName: String): Unit = {
    val maxDelay = UserConfig.streamingMonitoringMaxDeploy
    val notifyInterval = UserConfig.streamingMonitoringNotifyInterval
    if (maxDelay.isSuccess && notifyInterval.isSuccess) {
      ssc.addStreamingListener(
        new SparkStreamingMonitor(
          appName = appName,
          chainPhase = UserConfig.chainPhase,
          maxProcessingDelay = maxDelay.get.toMillis,
          notifyInterval = notifyInterval.get.toMillis
        )
      )
    }
  }
}
