package io.klaytn.apps.refining

import io.klaytn.model.InternalTransaction
import io.klaytn.utils.spark.{SparkHelper, UserConfig}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{SaveMode, functions}
import org.apache.spark.storage.StorageLevel

object RepartitionInternalTransactionBatch extends SparkHelper {
  override def run(args: Array[String]): Unit = {
    700 to 804 foreach { bnp =>
      val traceRDD = sc
        .textFile(
          s"s3a://${UserConfig.logStorageS3Path}/klaytn/label=kafka_log/topic=trace/bnp=$bnp/*.gz")
        .repartition(200)
        .flatMap { line =>
          InternalTransaction.parse(line) match {
            case Some(internalTransaction: InternalTransaction) =>
              internalTransaction.toRefined()
            case _ => None
          }
        }

      import spark.implicits._

      val internalTransactionDS = spark.createDataset(traceRDD)
      internalTransactionDS.persist(StorageLevel.MEMORY_ONLY)

      val labelUDF = udf { (label: String, suffix: String) =>
        label + "_" + suffix
      }

      val bnpUDF = udf { (blockNumber: Long) =>
        blockNumber / 1000
      }

//      val fpUDF = udf { (from: String) =>
//        Math.abs(from.hashCode) % 10000
//      }

      internalTransactionDS
        .where(col("from").isNotNull)
        .withColumn("bnp", bnpUDF(col("blockNumber")))
        .withColumn("label", labelUDF(col("label"), functions.lit("re_bnp")))
        .repartition(5)
        .write
        .mode(SaveMode.Append)
        .partitionBy("label", "bnp")
        .parquet(s"""s3a://${UserConfig.logStorageS3Path}/klaytn/""")

      internalTransactionDS
        .where(col("from").isNotNull)
        .withColumn("label", labelUDF(col("label"), functions.lit("re_fp")))
        .withColumn("fp", col("from").substr(39, 3))
        .drop("bnp")
        .write
        .mode(SaveMode.Append)
        .partitionBy("label", "fp")
        .parquet(s"""s3a://${UserConfig.logStorageS3Path}/klaytn/""")

      internalTransactionDS
        .where(col("from").isNotNull)
        .withColumn("label", labelUDF(col("label"), functions.lit("re_tp")))
        .withColumn("tp", col("to").substr(39, 3))
        .drop("bnp")
        .write
        .mode(SaveMode.Append)
        .partitionBy("label", "tp")
        .parquet(s"""s3a://${UserConfig.logStorageS3Path}/klaytn/""")

      //    internalTransactionDS
      //      .where(col("from").isNotNull)
      //      .withColumn("label", labelUDF(col("label"), functions.lit("re_fp")))
      ////      .withColumn("fp", fpUDF(col("from")))
      //      .withColumn("fp", col("from").substr(39, 4))
      //      .drop("bnp")
      //      .groupBy("fp")
      //      .count()
      //      .orderBy(col("count").desc)
      //      .show(3000, false)

      spark.catalog.clearCache()
      internalTransactionDS.unpersist()
    }
  }
}
