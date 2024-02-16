package io.klaytn.apps.adhoc.findoffset

import io.klaytn.client.SparkRedis
import io.klaytn.utils.spark.KafkaStreamingHelper

/*
--driver-cores 1 --driver-memory 3g
--num-executors 4 --executor-cores 4 --executor-memory 3g
--conf spark.streaming.kafka.maxRatePerPartition=500
--conf spark.app.phase=prod-cypress-modify-me
--class io.klaytn.apps.adhoc.findoffset.FindOffsetBlock
 */
object FindOffsetBlock extends KafkaStreamingHelper {
  override def run(args: Array[String]): Unit = {
    stream().foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        rdd
          .mapPartitions { iter =>
            iter.flatMap { r =>
              if (r.key().toLong < 85600000L) {
                Some((r.partition(), (r.key().toLong, r.offset())))
              } else if (r.key().toLong > 8600000) {
                SparkRedis.setex(s"FindOffset:${r.partition()}:stop", 3600, "1")
                if (SparkRedis.get("FindOffset:0:stop").isDefined &&
                    SparkRedis.get("FindOffset:1:stop").isDefined &&
                    SparkRedis.get("FindOffset:2:stop").isDefined &&
                    SparkRedis.get("FindOffset:3:stop").isDefined &&
                    SparkRedis.get("FindOffset:4:stop").isDefined &&
                    SparkRedis.get("FindOffset:5:stop").isDefined &&
                    SparkRedis.get("FindOffset:6:stop").isDefined &&
                    SparkRedis.get("FindOffset:7:stop").isDefined &&
                    SparkRedis.get("FindOffset:8:stop").isDefined &&
                    SparkRedis.get("FindOffset:9:stop").isDefined &&
                    SparkRedis.get("FindOffset:10:stop").isDefined &&
                    SparkRedis.get("FindOffset:11:stop").isDefined &&
                    SparkRedis.get("FindOffset:12:stop").isDefined &&
                    SparkRedis.get("FindOffset:13:stop").isDefined &&
                    SparkRedis.get("FindOffset:14:stop").isDefined) {
                  ssc.stop(stopSparkContext = true, stopGracefully = false)
                }
                None
              } else {
                None
              }
            }
          }
          .groupByKey(16)
          .map { x =>
            val a = x._2
            (x._1, a.toSeq.maxBy(_._2))
          }
          .repartition(1)
          .saveAsTextFile(
            s"gs://klaytn-spark-job/output/block_offset/${System.currentTimeMillis()}")
      }

      writeOffsetAndClearCache(rdd)
    }

  }
}
