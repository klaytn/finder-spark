package io.klaytn.client

import io.klaytn.utils.config.Cfg
import io.klaytn.utils.spark.UserConfig

object FinderRedis
    extends RedisClient("FinderRedis",
                        Cfg.getString("spark.app.redis.finder.url"),
                        Cfg.getInt("spark.app.redis.finder.timeout")) {

  override def makeKey(key: String): String =
    s"finder/${UserConfig.chainPhase.finderApiRedisPrefix}/$key"
}
