package io.klaytn.tools.client

import io.klaytn.datasource.redis.FinderRedisClient

case class RedisConfig(clientName: String,
                       url: String,
                       timeout: Int,
                       chainPhase: String)

object SparkRedis {
  def set(redisConfig: RedisConfig): FinderRedisClient = {
    new FinderRedisClient(redisConfig.clientName,
                          redisConfig.url,
                          redisConfig.timeout,
                          redisConfig.chainPhase)
  }

  private val CypressProdUrl = ""
  private val BaobabProdUrl = CypressProdUrl

  private val cypressConf =
    RedisConfig("CliCypressFinderRedis", CypressProdUrl, 3000, "prod-cypress")
  private val baobabConf =
    RedisConfig("CliBaobabFinderRedis", BaobabProdUrl, 3000, "prod-baobab")

  val cypressRedis: FinderRedisClient = set(cypressConf)
  val baobabRedis: FinderRedisClient = set(baobabConf)
}
