package io.klaytn.utils

import scala.concurrent.duration._
import scala.language.postfixOps

trait TimeUtil {
  // 90000    => 01m 30s
  // 39000000 => 10h 50m
  def readableTime(ttsInMills: Double): String = {
    val sec = (1 second).toMillis
    val min = (1 minute).toMillis
    val hour = (1 hour).toMillis

    if (ttsInMills < 1000) {
      f"$ttsInMills ms"
    } else if (ttsInMills < 1 * min) {
      val s = (ttsInMills / sec).toLong
      val ms = (ttsInMills % sec).toLong
      f"$s%02ds $ms%03d"
    } else if (ttsInMills < hour) {
      val m = (ttsInMills / min).toLong
      val s = ((ttsInMills % min) / sec).toLong
      f"$m%02dm $s%02ds"
    } else {
      val h = (ttsInMills / hour).toLong
      val m = ((ttsInMills % hour) / min).toLong
      f"$h%02dh $m%02dm"
    }
  }
}
