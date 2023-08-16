package io.klaytn.datasource.redis

import org.redisson.Redisson
import org.redisson.api.{
  RAtomicLong,
  RBucket,
  RBuckets,
  RMap,
  RScoredSortedSet,
  RSet,
  RTopic,
  RedissonClient
}
import org.redisson.config.Config

import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._

class RedisClient(clientName: String, nodeUrl: String, timeout: Int) {
  lazy val redissonClient: RedissonClient = {
    val config = new Config()

    config
      .useClusterServers()
      .addNodeAddress(s"redis://$nodeUrl")
      .setClientName(clientName)
      .setTimeout(timeout)
      .setScanInterval(10000)
      .setIdleConnectionTimeout(10000) // default: 10000
      .setConnectTimeout(10000) // default: 10000
      .setRetryAttempts(3) // default: 3
      .setRetryInterval(1500) // default: 1500
      .setMasterConnectionMinimumIdleSize(2) // default: 32
      .setMasterConnectionPoolSize(10) // default: 64
      .setSubscriptionsPerConnection(5) // default: 5
      .setSubscriptionConnectionMinimumIdleSize(1) // default: 1
      .setSubscriptionConnectionPoolSize(5) // default: 50

    config.setCodec(new org.redisson.client.codec.StringCodec())

    Redisson.create(config)
  }

  private def getBucket(key: String): RBucket[String] =
    redissonClient.getBucket[String](makeKey(key))

  private def getMap(key: String): RMap[String, String] =
    redissonClient.getMap[String, String](makeKey(key))

  private def getSet(key: String): RSet[String] =
    redissonClient.getSet[String](makeKey(key))

  private def getTopic(channel: String): RTopic =
    redissonClient.getTopic(makeKey(channel))

  private def getBuckets(): RBuckets = redissonClient.getBuckets()

  private def getAtomic(key: String): RAtomicLong =
    redissonClient.getAtomicLong(makeKey(key))

  private def getScoredSortedSet(key: String): RScoredSortedSet[String] =
    redissonClient.getScoredSortedSet[String](makeKey(key))

  def makeKey(key: String): String = key

  def get(key: String): Option[String] = Option(getBucket(key).get())

  def mget(keys: Seq[String]): Map[String, String] =
    getBuckets().get[String](keys: _*).asScala.toMap

  def set(key: String, value: String): Unit = getBucket(key).set(value)

  def mset(keyValues: Map[String, String]): Unit =
    getBuckets().set(keyValues.asJava)

  def setex(key: String, ttlSec: Long, value: String): Unit =
    getBucket(key).set(value, ttlSec, TimeUnit.SECONDS)

  def del(key: String): Option[Boolean] = Option(getBucket(key).delete())

  def expire(key: String, ttlSec: Long): Boolean =
    getBucket(key).expire(ttlSec, TimeUnit.SECONDS)

  def hget(key: String, field: String): Option[String] =
    Option(getMap(key).get(field))

  def hmget(key: String, fields: Set[String]): Map[String, String] =
    getMap(key).getAll(fields.asJava).asScala.toMap

  def hgetall(key: String): Map[String, String] =
    getMap(key).readAllMap().asScala.toMap

  def hset(key: String, field: String, value: String): Unit =
    getMap(key).put(field, value)

  def hmset(key: String, map: Map[String, String]): Unit =
    getMap(key).putAll(map.asJava)

  def hsetnx(key: String, field: String, value: String): Unit =
    getMap(key).putIfAbsent(field, value)

  def hsetnxex(key: String,
               ttlSec: Long,
               field: String,
               value: String): Unit = {
    getMap(key).putIfAbsent(field, value)
    getMap(key).expire(ttlSec, TimeUnit.SECONDS)
  }

  def hdel(key: String, field: String): Unit = getMap(key).remove(field)

  def publish(channel: String, message: String): Option[Long] =
    Option(getTopic(channel).publish(message))

  def sadd(key: String, member: String): Unit = getSet(key).add(member)

  def smembers(key: String): Set[String] = getSet(key).readAll().asScala.toSet

  def sismember(key: String, member: String): Boolean =
    getSet(key).contains(member)

  def srem(key: String, member: String): Unit = getSet(key).remove(member)

  def getset(key: String, newValue: String): Option[String] =
    Option(getBucket(key).getAndSet(newValue))

  def getsetex(key: String, ttlSec: Long, newValue: String): Option[String] =
    Option(getBucket(key).getAndSet(newValue, ttlSec, TimeUnit.SECONDS))

  def zadd(key: String, score: Long, member: String): Unit =
    getScoredSortedSet(key).add(score.toDouble, member)

  // Only add new elements. Don't update already existing elements.
  def zaddNX(key: String, score: Long, member: String): Unit =
    getScoredSortedSet(key).tryAdd(score.toDouble, member)

  def zrem(key: String, member: Seq[String]): Unit =
    getScoredSortedSet(key).removeAll(member.asJava)

  def zrangebyscore(key: String, minScore: Long, maxScore: Long): Seq[String] =
    getScoredSortedSet(key)
      .valueRange(minScore.toDouble, true, maxScore.toDouble, true)
      .asScala
      .toSeq

  def zrangebyscore(key: String,
                    minScore: Long,
                    maxScore: Long,
                    offset: Int,
                    count: Int): Seq[String] =
    getScoredSortedSet(key)
      .valueRange(minScore.toDouble,
                  true,
                  maxScore.toDouble,
                  true,
                  offset,
                  count)
      .asScala
      .toSeq

  def zremrangebyscore(key: String, minScore: Long, maxScore: Long): Unit =
    getScoredSortedSet(key).removeRangeByScore(minScore.toDouble,
                                               true,
                                               maxScore.toDouble,
                                               true)

  def incr(key: String): Long = getAtomic(key).incrementAndGet()

  def incrby(key: String, increment: Long): Long =
    getAtomic(key).addAndGet(increment)

  def decr(key: String): Long = getAtomic(key).decrementAndGet()

  def decrby(key: String, decrement: Long): Long =
    getAtomic(key).addAndGet(decrement * -1)
}
