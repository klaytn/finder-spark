package io.klaytn.utils.spark.offset

case class OffsetHistory(timestamp: Long, offsets: Array[OffsetMeta])
case class OffsetMeta(topic: String,
                      partition: Int,
                      fromOffset: Long,
                      untilOffset: Long)
