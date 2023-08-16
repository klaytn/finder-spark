package io.klaytn.client

import io.klaytn.utils.config.Cfg
import io.klaytn.utils.spark.UserConfig

object SparkRedis
    extends RedisClient("SparkRedis",
                        Cfg.getString("spark.app.redis.spark.url"),
                        Cfg.getInt("spark.app.redis.spark.timeout")) {

  override def makeKey(key: String): String =
    s"spark:app:${UserConfig.chainPhase}:$key"
}
