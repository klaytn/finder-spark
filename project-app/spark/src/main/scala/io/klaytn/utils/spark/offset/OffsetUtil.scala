package io.klaytn.utils.spark.offset

import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

import scala.collection.JavaConverters._

object OffsetUtil {
  val offsetManager = new S3OffsetManager()

  def readOffsetRanges(
      rdd: RDD[ConsumerRecord[String, String]]): Array[OffsetRange] = {
    rdd.asInstanceOf[HasOffsetRanges].offsetRanges
  }

  def writeOffset(bucket: String,
                  key: String,
                  offsetRanges: Array[OffsetRange]): Unit = {
    offsetManager.writeOffset(bucket, key, offsetRanges)
  }

  def readOffset(bucket: String, key: String): Array[OffsetRange] = {
    val offsetRanges = offsetManager.readOffset(bucket, key)
    offsetRanges
  }

  def readResumeOffsets(bucket: String,
                        key: String): Map[TopicPartition, Long] = {
    val offsetRanges = readOffset(bucket, key)
    offsetRanges.map { range =>
      new TopicPartition(range.topic, range.partition) -> range.untilOffset
    }.toMap
  }

  def readLatestOffsets(
      topics: Set[String],
      params: Map[String, Object]): Map[TopicPartition, Long] = {
    topics
      .map { topic =>
        readLatestOffsets(topic, params) // TODO open, close for topic number..
      }
      .reduce(_ ++ _)
  }

  def readLatestOffsetRanges(
      topic: String,
      params: Map[String, Object]): Array[OffsetRange] = {
    val latestParams = params + ("auto.offset.reset" -> "latest")
    val consumer = new KafkaConsumer[String, String](latestParams.asJava)
    try {
      val partitions = consumer.partitionsFor(topic)
      val topicPartitions = partitions.asScala.map(p => {
        new TopicPartition(p.topic(), p.partition())
      })
      consumer.assign(topicPartitions.asJavaCollection)
      consumer.seekToEnd(topicPartitions.asJavaCollection)
      val endOffsets = topicPartitions
        .map(tp => {
          Map(tp -> consumer.position(tp))
        })
        .reduce(_ ++ _)

      consumer.seekToBeginning(topicPartitions.asJavaCollection)
      val beginOffsets = topicPartitions
        .map(tp => {
          Map(tp -> consumer.position(tp))
        })
        .reduce(_ ++ _)

      endOffsets.map {
        case (topicPartitions, endOffset) =>
          val beginOffset = if (beginOffsets.contains(topicPartitions)) {
            beginOffsets(topicPartitions)
          } else {
            endOffset
          }

          OffsetRange(topicPartitions, beginOffset, endOffset)
      }.toArray
    } finally {
      consumer.close()
    }
  }

  def readLatestOffsets(
      topic: String,
      params: Map[String, Object]): Map[TopicPartition, Long] = {
    val latestParams = params + ("auto.offset.reset" -> "latest")
    val consumer = new KafkaConsumer[String, String](latestParams.asJava)
    try {
      val partitions = consumer.partitionsFor(topic)
      val topicPartitions = partitions.asScala.map(p => {
        new TopicPartition(p.topic(), p.partition())
      })
      consumer.assign(topicPartitions.asJavaCollection)
      consumer.seekToEnd(topicPartitions.asJavaCollection)
      val offsetsToRead = topicPartitions
        .map(tp => {
          Map(tp -> consumer.position(tp))
        })
        .reduce(_ ++ _)
      offsetsToRead
    } finally {
      consumer.close()
    }
  }

  def readBeginOffets(
      topic: String,
      params: Map[String, Object]): Map[TopicPartition, Long] = {
    val consumer = new KafkaConsumer[String, String](params.asJava)

    try {
      val partitions = consumer.partitionsFor(topic)
      val topicPartitions = partitions.asScala.map(p => {
        new TopicPartition(p.topic(), p.partition())
      })

      consumer.assign(topicPartitions.asJavaCollection)
      consumer.seekToBeginning(topicPartitions.asJavaCollection)
      val offsetToRead = topicPartitions
        .map(tp => {
          Map(tp -> consumer.position(tp))
        })
        .reduce(_ ++ _)
      offsetToRead
    } finally {
      consumer.close()
    }
  }
}
