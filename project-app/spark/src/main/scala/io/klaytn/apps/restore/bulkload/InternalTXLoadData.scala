package io.klaytn.apps.restore.bulkload

import io.klaytn.model.ChainPhase
import io.klaytn.service.LoadDataInfileService
import io.klaytn.utils.SlackUtil
import io.klaytn.utils.spark.SparkHelper

import scala.util.Random

/*
--driver-memory 10g
--num-executors 10
--executor-cores 4
--executor-memory 3g
--conf spark.app.phase=prod-cypress-modify-me
--class io.klaytn.apps.restore.bulkload.InternalTXLoadData
 */
object InternalTXLoadData extends SparkHelper with BulkLoadHelper {
  private val loadDataInfileService = LoadDataInfileService.of(ChainPhase.get())

  def loadData(bnp: Int): Unit = {
    val rdd = sc.textFile(
      s"gs://${outputDirPrefix()}/loadDataFromS3/list/trace/$bnp/part*")
    rdd
      .coalesce(1)
      .map { line =>
        val s = line.split("\t")
        val (filename, dbName) = (s(0), s(1))
        val index = dbName.replaceFirst("finder02", "").toInt - 1

        val key = if (filename.contains("/trace/")) {
          index * 4 + Random.nextInt(2) + 0
        } else {
          index * 4 + Random.nextInt(2) + 2
        }
        (key, line)
      }
      .repartition(8)
      .foreach {
        case (_, data) =>
          val bucket = outputBucket()

          val s = data.split("\t")
          val (filename, dbName) = (s(0), s(1))

          if (filename.contains("/trace/")) {
            loadDataInfileService.loadDataFromS3InternalTransaction(
              s"$bucket/$filename",
              dbName)
          } else if (filename.contains("/trace_index/")) {
            loadDataInfileService.loadDataFromS3InternalTransactionIndex(
              s"$bucket/$filename",
              dbName)
          }
      }
  }

  def makeMergeDataAndLoad(bnp: Int): Unit = {
    try {
      _makeMergeDataAndLoad(bnp)
    } catch {
      case e: Throwable =>
        if (e.getCause != null && e.getCause.getMessage.contains(
              "matches 0 files")) {
          // ignore empty directory
          e.printStackTrace()
        } else {
          SlackUtil.sendMessage(
            s"[${this.getClass.getSimpleName}] bnp=$bnp, error: ${e.getLocalizedMessage}")
          throw e
        }
    }
  }

  // Combine small data and load it all at once
  def _makeMergeDataAndLoad(bnp: Int): Unit = {
    sc.textFile(s"gs://${outputDirPrefix()}/loadDataFromS3/trace/$bnp/finder*")
      .repartition(1)
      .saveAsTextFile(
        s"gs://${outputDirPrefix()}/loadDataFromS3/trace_merge/$bnp")

    loadDataInfileService
      .loadDataFromS3InternalTransaction(
        s"${outputDirPrefix()}/loadDataFromS3/trace_merge/$bnp/part-00000",
        "finder0201")

    sc.textFile(
        s"gs://${outputDirPrefix()}/loadDataFromS3/trace_index/$bnp/finder*")
      .repartition(1)
      .saveAsTextFile(
        s"gs://${outputDirPrefix()}/loadDataFromS3/trace_index_merge/$bnp")
    loadDataInfileService
      .loadDataFromS3InternalTransactionIndex(
        s"${outputDirPrefix()}/loadDataFromS3/trace_index_merge/$bnp/part-00000",
        "finder0201")
  }

  override def run(args: Array[String]): Unit = {
    sendSlackMessage()
    start to end foreach { bnp =>
      loadData(bnp)
      makeMergeDataAndLoad(bnp)
    }
  }
}
