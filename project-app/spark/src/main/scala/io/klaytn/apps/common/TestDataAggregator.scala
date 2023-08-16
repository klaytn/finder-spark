package io.klaytn.apps.common

import scala.collection.mutable

object TestDataAggregator {
  val dataMap: mutable.Map[String, mutable.Map[String, String]] = mutable.Map()

  def set(partition: Int, key: String, totalSeg: Int, offset: Long): Unit = {
    val m =
      dataMap.getOrElseUpdate(partition.toString, mutable.Map[String, String]())
    m(key) = s"$offset\t$totalSeg"
  }

  def getAndClear(): String = {
    var size = 0
    dataMap.keySet.foreach(k => size += dataMap(k).size)
    if (size > 10) dataMap.clear()

    val result = dataMap.flatMap {
      case (k, v) =>
        v.map {
          case (k2, v2) =>
            s"$k\t$k2\t$v2"
        }
    } mkString "\n"

    s"id:${System.identityHashCode(dataMap)}\n$result\n"
  }
}
