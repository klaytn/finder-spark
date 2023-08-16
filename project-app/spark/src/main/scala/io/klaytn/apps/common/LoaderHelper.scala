package io.klaytn.apps.common

import io.klaytn.client.SparkRedis
import io.klaytn.model.{Block, InternalTransaction}
import io.klaytn.service.CaverService
import org.apache.kafka.clients.consumer.ConsumerRecord

import java.nio.ByteBuffer

object LoaderHelper {
  def getJson(r: ConsumerRecord[String, String]): Option[String] = {
    val totalSeg = ByteBuffer
      .wrap(r.headers().lastHeader("totalSegments").value())
      .getLong()
      .toInt

    if (totalSeg == 1) return Some(r.value)

    val segIdx = ByteBuffer
      .wrap(r.headers().lastHeader("segmentIdx").value())
      .getLong()
      .toInt
    val result = KafkaDataAggregator.setAndGet(r.key() + "_" + r.topic(),
                                               segIdx,
                                               totalSeg,
                                               r.value())
//    if (segIdx + 1 == totalSeg && result.isEmpty) {
//      SlackUtil.sendMessage(s"""get large data error] segIdx: $segIdx, totalSeg: $totalSeg,
//                               |map data: ${KafkaDataAggregator.dataMap.keySet.mkString(",")}""".stripMargin)
//    }
    result
  }

  def getInternalTransaction(
      iter: Seq[ConsumerRecord[String, String]]): Seq[InternalTransaction] = {
    // Since it is difficult to fetch internal txs that are not processed at once from the chain, use redis to correct them.
    iter.flatMap(getJson).flatMap(InternalTransaction.parse) ++
      KafkaDataAggregator.getRemainingData().flatMap(InternalTransaction.parse)
  }

  def getBlock(iter: Seq[ConsumerRecord[String, String]],
               redisKeyPrefix: String,
               caverService: CaverService): Seq[Block] = {
    // Process blocks that are not processed at once by fetching them directly from the chain.
    val block1 = iter.flatMap(getJson).flatMap(Block.parse)
    val block2 = KafkaDataAggregator
      .getKeysAndReset()
      .map(key => key.split("_")(0).toLong)
      .filter(blockNumber =>
        SparkRedis
          .getsetex(s"$redisKeyPrefix:$blockNumber", 3600, blockNumber.toString)
          .isEmpty)
      .map(caverService.getBlock)

    block1 ++ block2
  }

}
