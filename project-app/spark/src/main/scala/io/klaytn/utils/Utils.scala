package io.klaytn.utils

import io.klaytn.utils.config.Constants
import io.klaytn.utils.klaytn.NumberConverter.BigIntConverter

import scala.util.{Failure, Success, Try}

object Utils {
  def getDataFromDataURI(dataUri: String): String = {
    ""
  }

  def isMintByTransfer(from: String): Boolean = {
    from == Constants.ZeroAddress
  }

  def isBurnByTransfer(to: String): Boolean = {
    to == Constants.ZeroAddress || to == Constants.DeadAddress || to == Constants.KokoaDeadAddress
  }

  def getTotalSupplyOrder(decimal: Int, totalSupply: BigInt): String = {
    val fixedDecimal = 50 - decimal // Assume a maximum of 50 and match the digits
    BigInt(totalSupply + ("0" * fixedDecimal)).to128BitsHex()
  }

  def getDisplayOrder(blockNumber: Long,
                      transactionIndex: Int,
                      eventIndex: Int): String = {
    val blockNumberStr =
      f"${BigInt(blockNumber).toString(16)}%12s".replace(' ', '0')
    val txIndexStr =
      f"${BigInt(transactionIndex).toString(16)}%5s".replace(' ', '0')
    val eventIndexStr =
      f"${BigInt(eventIndex).toString(16)}%5s".replace(' ', '0')

    s"$blockNumberStr$txIndexStr$eventIndexStr"
  }

  def getBlockNumberPartition(blockNumber: Long): String = {
    (blockNumber / 100000).toString
  }

  @annotation.tailrec
  def retry[T](n: Int, sleepMs: Long)(fn: => T): T = {
    Try { fn } match {
      case Success(x) => x
      case _ if n > 1 =>
        if (sleepMs > 0) {
          Thread.sleep(sleepMs)
        }
        retry(n - 1, sleepMs)(fn)
      case Failure(e) => throw e
    }
  }

  def retryAndReportOnFail[T](
      n: Int,
      sleepMs: Long,
      errorReport: Throwable => Unit)(fn: => T): Option[T] = {
    try {
      val t = retry(n, sleepMs)(fn)
      Some(t)
    } catch {
      case e: Throwable =>
        errorReport(e)
        None
    }
  }
}
