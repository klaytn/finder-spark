package io.klaytn.datasource.redis

import io.klaytn.datasource.redis.RedisClient

class SparkRedisClient(clientName: String,
                       nodeUrl: String,
                       timeout: Int,
                       keyPrefix: String)
    extends RedisClient(clientName, nodeUrl, timeout) {

  override def makeKey(key: String): String = s"finder/$keyPrefix/$key"
}
