package io.klaytn.utils.spark

import io.klaytn.utils.SlackUtil
import io.klaytn.utils.spark.offset.OffsetUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{
  ConsumerStrategies,
  KafkaUtils,
  LocationStrategies
}

trait KafkaStreamingHelper extends StreamingHelper {
  def consumerGroupId(): String = s"$appName-${UserConfig.chainPhase}"
  def topics(): Set[String] = UserConfig.useTopics
  def bootstrapServers(): String = UserConfig.bootstrapServers
  def offsetBucket(): String = UserConfig.baseBucket
  def offsetKey(): String = s"$jobBasePath/${UserConfig.offsetMetaKey}"
  def consumeMode(): String = UserConfig.consumeMode

  // TODO: Automtically configured spark.streaming.ui.retainedBatches --> manage explicitly in config file
  def createKafkaParams(consumerGroupId: String,
                        bootstrapServers: String): Map[String, Object] = {
    Map[String, Object](
      "bootstrap.servers" -> bootstrapServers,
      "group.id" -> consumerGroupId,
      "max.poll.records" -> (500: java.lang.Integer),
      "enable.auto.commit" -> (false: java.lang.Boolean), // control the offset directly for direct streaming
      "auto.offset.reset" -> "earliest", // To avoid missing newly added partition data, we'll use the
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer]
    )
  }

  def createDirectStream(
      topics: Set[String],
      ssc: StreamingContext,
      consumerGroupId: String,
      bootstrapServers: String,
      offsetBucket: String,
      offsetKey: String,
      mode: String = "resume"): InputDStream[ConsumerRecord[String, String]] = {

    val kafkaParams: Map[String, Object] =
      createKafkaParams(consumerGroupId, bootstrapServers)

    val offsetsRead: Map[TopicPartition, Long] = mode match {
      case "resume" =>
        val resumeOffset =
          OffsetUtil.readResumeOffsets(offsetBucket, offsetKey)
        if (resumeOffset.isEmpty) {
          OffsetUtil.readLatestOffsets(topics, kafkaParams)
        } else {
          resumeOffset
        }
      case "latest"   => OffsetUtil.readLatestOffsets(topics, kafkaParams)
      case "earliest" => Map.empty
    }

    KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies
        .Subscribe[String, String](topics, kafkaParams, offsetsRead))
  }

  def stream(): InputDStream[ConsumerRecord[String, String]] = {
    createDirectStream(this.topics(),
                       ssc,
                       this.consumerGroupId(),
                       this.bootstrapServers(),
                       this.offsetBucket(),
                       this.offsetKey(),
                       this.consumeMode())
  }

  def writeOffset(rdd: RDD[ConsumerRecord[String, String]]): Unit = {
    OffsetUtil.writeOffset(offsetBucket(),
                           offsetKey(),
                           OffsetUtil.readOffsetRanges(rdd))
  }

  def writeOffsetAndClearCache(
      rdd: RDD[ConsumerRecord[String, String]]): Unit = {
    if (!rdd.isEmpty()) {
      writeOffset(rdd)
      spark.catalog.clearCache()
      spark.sparkContext.getPersistentRDDs.foreach(_._2.unpersist())
    }
  }

  override def startMessage(): String = {
    s"""
       |${super.startMessage()}
       |""".stripMargin
  }
}
