package io.klaytn.apps.adhoc.checking

import io.klaytn.utils.SlackUtil
import io.klaytn.utils.spark.SparkHelper
import org.apache.spark.sql.functions.col

import java.math.BigInteger
import scala.collection.mutable

object CheckPartitionBatch extends SparkHelper {
  def shardNo(shardKey: String, x: Int): Int = {
    try {
      if (x > 0) {
        val len = shardKey.length
        val value = new BigInteger(shardKey.substring(len - x), 16)
        value.mod(BigInteger.valueOf(10)).intValue()
      } else {
        val value = new BigInteger(shardKey.substring(2), 16)
        value.mod(BigInteger.valueOf(10)).intValue()
      }
    } catch {
      case _: Throwable =>
        -1
    }
  }

  def a(): Unit = {
    val sourceDataPath: String =
      "s3a://klaytn-prod-lake/klaytn/label=internal_transactions/"
    val df = spark.read.parquet(sourceDataPath)
    val res = df
      .select(col("from"), col("to"))
      .rdd
      .mapPartitions { iter =>
        val m = mutable.Map.empty[String, Long]
        iter.foreach { row =>
          val from = row.getString(0)
          val to = row.getString(1)

          if (from != null && from.nonEmpty) {
            val sno1 = s"${shardNo(from, 2)}_2"
            val sno1Sum = m.getOrElse(sno1, 0L) + 1
            m.put(sno1, sno1Sum)

            val sno11 = s"${shardNo(from, 3)}_3"
            val sno11Sum = m.getOrElse(sno11, 0L) + 1
            m.put(sno11, sno11Sum)
          }

          if (to != null && to.nonEmpty) {
            val sno2 = s"${shardNo(to, 2)}_2"
            val sno2Sum = m.getOrElse(sno2, 0L) + 1
            m.put(sno2, sno2Sum)

            val sno22 = s"${shardNo(to, 3)}_3"
            val sno22Sum = m.getOrElse(sno22, 0L) + 1
            m.put(sno22, sno22Sum)
          }
        }
        m.keysIterator.map { k =>
          (k, m(k))
        }
      }
      .reduceByKey(_ + _)
      .collect()
      .sorted

    val msg = res
      .map {
        case (k, v) =>
          s"$k : $v"
      }
      .mkString("\n")

    SlackUtil.sendMessage("type1")
    SlackUtil.sendMessage(msg)
  }

  def b(): Unit = {
    val sourceDataPath: String =
      "s3a://klaytn-prod-lake/klaytn/label=internal_transactions/"
    val df = spark.read.parquet(sourceDataPath)
    val res = df
      .select(col("from"), col("to"))
      .rdd
      .mapPartitions { iter =>
        val m = mutable.Map.empty[String, mutable.Set[String]]
        iter.foreach { row =>
          val from = row.getString(0)
          val to = row.getString(1)

          val key1 = s"${shardNo(from, 2)}_2"
          val key2 = s"${shardNo(to, 2)}_2"
          val key3 = s"${shardNo(from, 3)}_3"
          val key4 = s"${shardNo(to, 3)}_3"
          val key5 = s"${shardNo(from, 0)}_0"
          val key6 = s"${shardNo(to, 0)}_0"

          val s1 = m.getOrElse(from, mutable.Set.empty[String])
          val s2 = m.getOrElse(to, mutable.Set.empty[String])
          s1.add(key1)
          s1.add(key3)
          s1.add(key5)

          s2.add(key2)
          s2.add(key4)
          s2.add(key6)

          m.put(from, s1)
          m.put(to, s2)
        }
        m.keys.flatMap { k =>
          m(k).map { v =>
            (k, v)
          }
        }.iterator
      }
      .groupByKey()
      .map { x =>
        (x._2.head, x._1)
      }
      .groupByKey()
      .map { x =>
        (x._1, x._2.size)
      }
      .collect()

    val msg = res
      .map {
        case (k, v) =>
          s"$k : $v"
      }
      .mkString("\n")

    SlackUtil.sendMessage("type2")
    SlackUtil.sendMessage(msg)
  }

  override def run(args: Array[String]): Unit = {
    //  a
    b
  }
}
