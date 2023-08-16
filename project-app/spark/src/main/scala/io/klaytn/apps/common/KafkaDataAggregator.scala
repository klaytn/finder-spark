package io.klaytn.apps.common

import io.klaytn.client.SparkRedis

import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable
import scala.jdk.CollectionConverters.mapAsScalaConcurrentMapConverter

object KafkaDataAggregator {
  private val dataMap: mutable.Map[String, mutable.Map[Int, String]] =
    new ConcurrentHashMap[String, mutable.Map[Int, String]]() asScala
//  val removeKeys: mutable.Set[String] =
//    Collections.newSetFromMap(new ConcurrentHashMap[String, java.lang.Boolean]()) asScala

  def setAndGet(key: String,
                index: Int,
                total: Int,
                data: String): Option[String] = {
    val m =
      dataMap.getOrElseUpdate(key, new ConcurrentHashMap[Int, String]() asScala)
    m(index) = data
    m(-1) = total.toString

    if (m.size != total + 1) return None

    val result = (0 until total).map(idx => m.getOrElse(idx, "")).mkString
    dataMap.remove(key)
//    removeKeys.add(key)
    Some(result)
  }

  def removeKey(key: String): Unit = dataMap.remove(key)

  def getKeysAndReset(): Set[String] = {
    val result = dataMap.keySet.toSet
    dataMap.clear()
    result
  }

  def getRemainingData(): Seq[String] = {
    if (dataMap.isEmpty) return Seq.empty[String]

//    SlackUtil.sendMessage("getRemainingData")
    dataMap.foreach {
      case (key, map) =>
        val total = map(-1).toInt
        val getFields = (0 until total)
          .filter(idx => !map.contains(idx))
          .map(_.toString)
          .toSet
        SparkRedis.hmget(key, getFields).foreach {
          case (k, v) => map(k.toInt) = v
        }
    }

    val filteredDataMap = dataMap.filter {
      case (k, v) => v(-1).toInt + 1 == v.size
    }

    // fetch the value from redis and process the completed data
    val result = filteredDataMap.map {
      case (key, m) =>
        val r = (0 until m(-1).toInt).map(idx => m.getOrElse(idx, "")).mkString
        dataMap.remove(key)
        r
    } toSeq

    // Save the remaining data to a redis (in case the job is restarted, etc.)
    dataMap.foreach {
      case (key, value) =>
        value.foreach {
          case (index, data) =>
            SparkRedis.hsetnxex(key, 3600, index.toString, data)
        }
    }

    result
  }

//  def checkMapAndGetall(partition: Int): Seq[DumpKafkaLog] = {
//    val result = dataMap.flatMap {
//      case (key, value) =>
//        if (key.contains(s":$partition") && value.size == value(-1).toInt + 1) {
//          val log = (0 until value(-1).toInt).map(idx => value.getOrElse(idx, "")).mkString
//          removeKeys.add(key)
//          Some(DumpKafkaLog(Utils.getBlockNumberPartition(key.toLong), log))
//        } else {
//          None
//        }
//    }.toSeq
//
//    removeKeys.foreach(key => dataMap.remove(key))
//
//    result
//  }
}
