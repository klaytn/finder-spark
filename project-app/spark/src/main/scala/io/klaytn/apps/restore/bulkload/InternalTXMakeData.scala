package io.klaytn.apps.restore.bulkload

import io.klaytn.model.{ChainPhase, InternalTransaction}
import io.klaytn.persistent.InternalTransactionPersistentAPI
import io.klaytn.service.LoadDataInfileService
import io.klaytn.utils.SlackUtil
import io.klaytn.utils.gcs.GCSUtil
import io.klaytn.utils.spark.SparkHelper
import org.apache.spark.TaskContext

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/*
--driver-memory 10g
--num-executors 64
--executor-cores 4
--executor-memory 3g
--conf spark.app.phase=prod-cypress-modify-me
--class io.klaytn.apps.restore.bulkload.InternalTXMakeData
 */
object InternalTXMakeData extends SparkHelper with BulkLoadHelper {
  private val chainPhase = ChainPhase.get()
  private val internalTransactionPersistentAPI =
    InternalTransactionPersistentAPI.of(chainPhase)
  private val loadDataInfileService = LoadDataInfileService.of(chainPhase)

  def procMakeData(bnp: Int, numPartition: Int): Unit = {
    try {
      _procMakeData(bnp, numPartition)
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
  def _procMakeData(bnp: Int, numPartition: Int): Unit = {
    val path = s"gs://${kafkaLogDirPrefix()}/topic=trace/bnp=$bnp/*.gz"
    val rdd = sc.textFile(path)
    rdd
      .repartition(256)
      .flatMap { line =>
        InternalTransaction.parse(line) match {
          case Some(itx) =>
            val (itxLines, itxIndexLines) =
              loadDataInfileService.internalTransactionLines(itx.toRefined())

            itxLines.map { x =>
              val dbName =
                internalTransactionPersistentAPI.getInternalTransactionDB(
                  itx.blockNumber)
              (s"${Random.nextInt(100000)}_t_$dbName", x)
            } ++
              itxIndexLines.map {
                case (address, line) =>
                  val dbName = internalTransactionPersistentAPI
                    .getInternalTransactionIndexDB(address)
                  (s"${Random.nextInt(100000)}_i_$dbName", line)
              }
          case _ => Seq.empty
        }
      }
      .repartition(numPartition)
      .mapPartitions { data =>
        val result = ArrayBuffer.empty[String]

        val m = mutable.Map.empty[String, ArrayBuffer[String]]
        data.foreach {
          case (k, v) =>
            val s = k.split("_")
            val (typ, dbName) = (s(1), s(2))
            m.getOrElseUpdate(s"${typ}_$dbName", ArrayBuffer.empty[String])
              .append(v)
        }

        val bucket = outputBucket()
        val partitionId = TaskContext.getPartitionId()

        m.foreach {
          case (k, v) =>
            val s = k.split("_")
            val (typ, dbName) = (s(0), s(1))

            if (typ == "t") {
              v.grouped(75000).zipWithIndex.foreach {
                case (data, index) =>
                  val keyITX =
                    s"${outputKeyPrefix()}/loadDataFromS3/trace/$bnp/$dbName.$partitionId.$index"
                  GCSUtil.writeText(bucket, keyITX, s"${data.mkString("\n")}\n")
                  result.append(s"$keyITX\t$dbName")
              }
            } else if (typ == "i") {
              v.grouped(150000).zipWithIndex.foreach {
                case (data, index) =>
                  val keyITXIndex =
                    s"${outputKeyPrefix()}/loadDataFromS3/trace_index/$bnp/$dbName.$partitionId.$index"
                  GCSUtil.writeText(bucket,
                                    keyITXIndex,
                                    s"${data.mkString("\n")}\n")
                  result.append(s"$keyITXIndex\t$dbName")
              }
            }
        }

        result.iterator
      }
      .repartition(16)
      .saveAsTextFile(
        s"gs://${outputDirPrefix()}/loadDataFromS3/list/trace/$bnp")
  }

  override def run(args: Array[String]): Unit = {
    sendSlackMessage()
    start to end foreach { bnp =>
      procMakeData(bnp, 512)
    }
  }
}
