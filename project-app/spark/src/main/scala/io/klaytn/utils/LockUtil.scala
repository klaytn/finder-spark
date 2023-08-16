package io.klaytn.utils

import io.klaytn.client.SparkRedis

object LockUtil extends Serializable {
  private val KeyPrefix = "lock"

  val redissonClient = SparkRedis.redissonClient

  def lock[T](key: String)(supplier: () => T): T = {
    lock(key, 5 * 60 * 1000, 10 * 1000)(supplier)
  }

  def lock[T](key: String, waitTimeMs: Long, leaseTimeMs: Long)(
      supplier: () => T): T = {
    val lock = redissonClient.getLock(SparkRedis.makeKey(s"$KeyPrefix:$key"))
    try {
      if (lock.tryLock(waitTimeMs,
                       leaseTimeMs,
                       java.util.concurrent.TimeUnit.MILLISECONDS)) {
        try {
          supplier()
        } finally {
          lock.unlock()
        }
      } else {
        if (lock != null && lock.isLocked && lock.isHeldByCurrentThread) {
          lock.unlock()
        }
        supplier()
      }
    } catch {
      case e: Throwable => throw e
    }
  }
}
