package io.klaytn.apps.adhoc.account

import io.klaytn.model.Block
import io.klaytn.utils.gcs.GCSUtil
import io.klaytn.utils.spark.{SparkHelper, UserConfig}

object FindAccountCreatedAtBatch extends SparkHelper {
  override def run(args: Array[String]): Unit = {
    0 to 1000 foreach { bnp =>
      sc.textFile(
          s"gs://klaytn-prod-lake/klaytn/${UserConfig.chainPhase.chain}/label=kafka_log/topic=block/bnp=$bnp/*.gz")
        .flatMap(Block.parse)
        .flatMap(_.toRefined._2)
        .flatMap { tx =>
          if (tx.to.isDefined)
            Seq((tx.from, tx.timestamp), (tx.to.get, tx.timestamp))
          else Seq((tx.from, tx.timestamp))
        }
        .groupByKey(8192)
        .map {
          case (key, values) =>
            s"$key\t${values.min}"
        }
        .repartition(32)
        .saveAsTextFile(
          s"gs://klaytn-prod-spark/output/adhoc/account-created-at/${UserConfig.chainPhase.chain}/bnp=$bnp/")
    }

    GCSUtil.delete(
      "klaytn-prod-spark",
      s"/output/adhoc/account-created-at/${UserConfig.chainPhase.chain}/last",
      true)

    sc.textFile(
        s"gs://klaytn-prod-spark/output/adhoc/account-created-at/${UserConfig.chainPhase.chain}/bnp=*/part*")
      .map { line =>
        val s = line.split("\t")
        (s.head, s.last.toInt)
      }
      .groupByKey(512)
      .map {
        case (key, values) =>
          s"$key\t${values.min}"
      }
      .repartition(32)
      .saveAsTextFile(
        s"gs://klaytn-prod-spark/output/adhoc/account-created-at/${UserConfig.chainPhase.chain}/last/")
  }
}
