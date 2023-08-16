package io.klaytn.utils.spark.offset

import org.apache.spark.streaming.kafka010.OffsetRange

trait OffsetManager {
  def writeOffset(offsetMetaFilePath: String, offsetRanges: Array[OffsetRange])
  def readOffset(offsetMetaFilePath: String): Array[OffsetRange]
}
