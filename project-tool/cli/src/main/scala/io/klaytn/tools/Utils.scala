package io.klaytn.tools

import io.klaytn.datasource.redis.FinderRedisClient
import io.klaytn.tools.client.SparkRedis
import io.klaytn.tools.model.ServiceType

import scala.util.Try

object Utils {
  def getType(args: Array[String]): ServiceType.Value = {
    Try { args.head }.toOption
      .flatMap(ServiceType.of)
      .getOrElse(ServiceType.klaytn)
  }
  def getPhaseAndRedisClients(
      typ: ServiceType.Value): Seq[(String, FinderRedisClient)] = {
    typ match {
      case ServiceType.klaytn =>
        Seq(("baobab", SparkRedis.baobabRedis),
            ("cypress", SparkRedis.cypressRedis))
      case _ => throw new RuntimeException(s"not support type: $typ")
    }
  }
}
