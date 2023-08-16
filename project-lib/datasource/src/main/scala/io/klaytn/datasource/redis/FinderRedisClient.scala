package io.klaytn.datasource.redis

import io.klaytn.datasource.redis.RedisClient

class FinderRedisClient(clientName: String,
                        nodeUrl: String,
                        timeout: Int,
                        chainPhase: String)
    extends RedisClient(clientName, nodeUrl, timeout) {

  override def makeKey(key: String): String = s"spark:app:$chainPhase:$key"
}
