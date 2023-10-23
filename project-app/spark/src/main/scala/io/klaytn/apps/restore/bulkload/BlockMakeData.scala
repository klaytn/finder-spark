package io.klaytn.apps.restore.bulkload

import io.klaytn.model.{Block, ChainPhase}
import io.klaytn.service.LoadDataInfileService
import io.klaytn.utils.gcs.GCSUtil
import io.klaytn.utils.spark.SparkHelper
import org.apache.spark.TaskContext

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object BlockMakeData extends SparkHelper with BulkLoadHelper {
  private val loadDataInfileService = LoadDataInfileService.of(ChainPhase.get())

  def procMakeData(bnp: Int, numPartition: Int): Unit = {
    val rdd =
      sc.textFile(s"s3a://${kafkaLogDirPrefix()}/topic=block/bnp=$bnp/*.gz")
    rdd
      .repartition(256)
      .flatMap { line =>
        Block.parse(line) match {
          case Some(block0) =>
            val refinedData = block0.toRefined

            val (block, transactionReceipts, eventLogs) =
              (refinedData._1, refinedData._2, refinedData._3)

            val blockLine = loadDataInfileService.blockLine(block)
            val transactionLines =
              loadDataInfileService.transactionReceiptLines(transactionReceipts)
            val eventLogLines = loadDataInfileService.eventLogLines(eventLogs)

            Seq((s"${Random.nextInt(100000)}_b", blockLine)) ++
              transactionLines.map(x => (s"${Random.nextInt(100000)}_t", x)) ++
              eventLogLines.map(x => (s"${Random.nextInt(100000)}_e", x))
          case _ => Seq.empty
        }
      }
      .repartition(numPartition)
      .mapPartitions { data =>
        val result = ArrayBuffer.empty[String]

        val m = mutable.Map.empty[String, ArrayBuffer[String]]
        data.foreach {
          case (k, v) =>
            val typ = k.split("_")(1)
            m.getOrElseUpdate(typ, ArrayBuffer.empty[String]).append(v)
        }

        val bucket = outputBucket()
        val partitionId = TaskContext.getPartitionId()

        if (m.contains("b") && m("b").nonEmpty) {
          val keyBlock =
            s"${outputKeyPrefix()}/loadDataFromS3/block/$bnp/$partitionId"
          GCSUtil.writeText(bucket, keyBlock, s"${m("b").mkString("\n")}\n")
          result.append(keyBlock)
        }

        if (m.contains("t") && m("t").nonEmpty) {
          m("t").grouped(10000).zipWithIndex.foreach {
            case (data, index) =>
              val keyTx =
                s"${outputKeyPrefix()}/loadDataFromS3/tx/$bnp/$partitionId.$index"
              GCSUtil.writeText(bucket, keyTx, s"${data.mkString("\n")}\n")
              result.append(keyTx)
          }
        }
        if (m.contains("e") && m("e").nonEmpty) {
          m("e").grouped(20000).zipWithIndex.foreach {
            case (data, index) =>
              val keyEventLog =
                s"${outputKeyPrefix()}/loadDataFromS3/eventlog/$bnp/$partitionId.$index"
              GCSUtil.writeText(bucket,
                                keyEventLog,
                                s"${data.mkString("\n")}\n")
              result.append(keyEventLog)
          }
        }

        result.iterator
      }
      .repartition(16)
      .saveAsTextFile(
        s"s3a://${outputDirPrefix()}/loadDataFromS3/list/block/$bnp")
  }

  override def run(args: Array[String]): Unit = {
    sendSlackMessage()
    start to end foreach { bnp =>
      procMakeData(bnp, 512)
    }
  }
}
