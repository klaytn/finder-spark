package io.klaytn.apps.refining

import io.klaytn.utils.spark.SparkHelper
import org.apache.hadoop.io.compress.GzipCodec
import io.klaytn.utils.spark.{SparkHelper, UserConfig}

object RestoreKafkaLogBatch extends SparkHelper {
  override def run(args: Array[String]): Unit = {
    Seq("block", "trace").foreach { topic =>
      0 to 1260 foreach { bnp =>
        val logPath =
          s"gs://${UserConfig.logStorageS3Path}/klaytn/cypress/label=kafka_log/topic=$topic/bnp=$bnp/*.gz"
        sc.textFile(logPath)
          .repartition(30)
          .saveAsTextFile(
            s"gs://test/klaytn/cypress/label=kafka_log/topic=$topic/bnp=$bnp",
            classOf[GzipCodec])
      }
    }

  }
}
