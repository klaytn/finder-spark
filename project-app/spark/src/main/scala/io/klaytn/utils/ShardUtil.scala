package io.klaytn.utils

import java.math.BigInteger

object ShardUtil {
  def blockNumberShardNumSelector(blockNumber: Long, shardCount: Int): Int = {
    (blockNumber % shardCount).toInt + 1
  }

  def accountAddressShardNumSelector(accountAddress: String,
                                     shardCount: Int): Int = {
    val value = new BigInteger(accountAddress.substring(2), 16)
    val shardIndex = value.mod(BigInteger.valueOf(shardCount.longValue()))
    shardIndex.intValue() + 1
  }
}
