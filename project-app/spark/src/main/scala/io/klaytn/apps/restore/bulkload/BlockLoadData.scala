package io.klaytn.apps.restore.bulkload

import io.klaytn.model.ChainPhase
import io.klaytn.service.LoadDataInfileService
import io.klaytn.utils.spark.SparkHelper

/*
--driver-memory 10g
--num-executors 2
--executor-cores 4
--executor-memory 3g
--conf spark.app.phase=prod-cypress-modify-me
--class io.klaytn.apps.restore.bulkload.BlockLoadData
 */
object BlockLoadData extends SparkHelper with BulkLoadHelper {
  private val loadDataInfileService = LoadDataInfileService.of(ChainPhase.get())

  def loadData(bnp: Int): Unit = {
    val rdd = sc.textFile(
      s"gs://${outputDirPrefix()}/loadDataFromS3/list/block/$bnp/part*")

    rdd.repartition(8).foreach { filename =>
      val bucket = outputBucket()

      if (filename.contains("/block/")) {
        loadDataInfileService.loadDataFromS3Block(s"$bucket/$filename")
      } else if (filename.contains("/tx/")) {
        loadDataInfileService.loadDataFromS3Transaction(s"$bucket/$filename")
      } else if (filename.contains("/eventlog/")) {
        loadDataInfileService.loadDataFromS3EventLog(s"$bucket/$filename")
      }
    }
  }

  def loadEventLogData(bnp: Int): Unit = {
    val rdd = sc.textFile(
      s"gs://${outputDirPrefix()}/loadDataFromS3/list/block/$bnp/part*")

    rdd.repartition(8).foreach { filename =>
      val bucket = outputBucket()

      if (filename.contains("/eventlog/")) {
        loadDataInfileService.loadDataFromS3EventLog(s"$bucket/$filename")
      }
    }
  }

  override def run(args: Array[String]): Unit = {
    sendSlackMessage()

    start to end foreach { bnp =>
      loadData(bnp)
    }
  }
}
