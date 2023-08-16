package io.klaytn.apps.worker

import io.klaytn.utils.spark.UserConfig
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

class WorkerMockReceiver(numWorkers: Int)
    extends Receiver[String](StorageLevel.MEMORY_ONLY)
    with Logging {
  val batchDuration: Long = UserConfig.batchDuration.milliseconds

  def receive(): Unit = {
    while (!isStopped()) {
      val start = System.currentTimeMillis()
      1 to numWorkers foreach { i =>
        store(i.toString)
      }
      val time = start - System.currentTimeMillis()
      Thread.sleep(Math.min(batchDuration, batchDuration - time))
    }
  }

  override def onStart(): Unit = {
    new Thread("Socket Receiver") {
      setDaemon(true)

      override def run(): Unit = {
        receive()
      }
    }.start()
  }

  override def onStop(): Unit = {}
}
