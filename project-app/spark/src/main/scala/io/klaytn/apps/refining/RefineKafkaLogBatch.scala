package io.klaytn.apps.refining

import io.klaytn.model.DumpKafkaLog
import io.klaytn.utils.Utils
import io.klaytn.utils.spark.{SparkHelper, UserConfig}
import org.apache.spark.sql.SaveMode

// Deduplicate kafka logs and merge manual recovery logs
object RefineKafkaLogBatch extends SparkHelper {
  override def run(args: Array[String]): Unit = {
    Seq("block", "trace") foreach { topic =>
      862 to 1083 foreach { bnp =>
        val bBnp = sc.broadcast(bnp)
        val originLogPath =
          s"gs://${UserConfig.logStorageS3Path}/klaytn/label=kafka_log/topic=$topic/bnp=$bnp/*.gz"
        val blockRDD = sc
          .textFile(s"$originLogPath", 150)
          .flatMap { line =>
            val s = line.split(",")
            try {
              if (s.head.startsWith("{\"blockNumber\":")) {
                val Array(_, strNumber) = s.head.split(":")
                val blockNumber = strNumber.toLong
                if (Utils
                      .getBlockNumberPartition(blockNumber)
                      .toInt == bBnp.value) {
                  Some((blockNumber, line))
                } else {
                  None
                }
              } else {
                Some((-1L, line))
              }
            } catch {
              case _: Throwable =>
                Some((-1L, line))
            }
          }
          .groupByKey(150)
          .flatMap {
            case (blockNumber, lines) =>
              val bnp = Utils.getBlockNumberPartition(blockNumber)
              if (blockNumber >= 0) {
                val lineSet = lines.toSet
                if (lineSet.size == 1) {
                  List(DumpKafkaLog(bnp, lineSet.head))
                } else {
                  val pattern = """"time":"[^"]*"""".r
                  val tmp = lineSet.map(line => pattern.replaceAllIn(line, ""))
                  if (tmp.size == 1) {
                    List(DumpKafkaLog(bnp, lineSet.head))
                  } else if (tmp.size == 2) {
                    val a = lineSet
                      .map { line =>
                        line.length
                      }
                      .toList
                      .sorted
                      .min
                    List(
                      DumpKafkaLog(bnp,
                                   lineSet.filter(x => x.length == a).head))
                  } else {
                    tmp.map(line => DumpKafkaLog(s"dup-$bnp", line))
                  }
                }
              } else {
                lines.map(line => DumpKafkaLog(bnp, line))
              }
          }

        import spark.implicits._
        val ds = spark.createDataset(blockRDD)
        ds.repartition(30)
          .write
          .mode(SaveMode.Append)
          .option("compression", "gzip")
          .partitionBy("bnp")
          .text(
            s"gs://${UserConfig.logStorageS3Path}/klaytn/label=kafka_log/topic=$topic/")
      }
    }
  }
}
