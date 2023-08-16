package io.klaytn.model

import io.klaytn.utils.klaytn.NumberConverter._
import io.klaytn.utils.JsonUtil.Implicits._
import io.klaytn.utils.{JsonUtil, Utils}

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ArrayBuffer

case class InternalTransaction(blockNumber: Long,
                               result: List[InternalTransactionContent]) {
  def parseCalls(
      parentCallId: Option[Int],
      callId: AtomicInteger,
      calls: List[InternalTransactionCalls]): List[InternalTransactionCalls] = {
    calls.flatMap { it =>
      it.callId = Some(callId.getAndIncrement())
      it.parentCallId = parentCallId

      val list = ArrayBuffer[InternalTransactionCalls]()
      if (it.calls.getOrElse(List.empty).nonEmpty) {
        list.appendAll(parseCalls(it.callId, callId, it.calls.get))
      }

      list.append(it)
      list
    }
  }

  def toRefined(): List[RefinedInternalTransactions] = {
    val blockNumberPartition = Utils.getBlockNumberPartition(blockNumber)
    result.zipWithIndex.flatMap {
      case (i, idx) =>
        val gas = i.gas match {
          case Some(gas) => Some(gas.hexToLong())
          case _         => None
        }
        val gasUsed = i.gasUsed match {
          case Some(gasUsed) => Some(gasUsed.hexToInt())
          case _             => None
        }

        val value = i.value match {
          case Some(value) => Some(value.hexToBigInt().toString)
          case _           => None
        }

        val defaultInternalTransaction =
          RefinedInternalTransactions(
            "internal_transactions",
            blockNumberPartition,
            blockNumber,
            idx,
            0,
            None,
            i.`type`,
            i.from,
            i.to,
            value,
            gas,
            gasUsed,
            i.input,
            i.output,
            i.time,
            i.error,
            i.reverted
          )

        val calls = i.calls match {
          case Some(calls) =>
            val parentCallId = Some(0)
            val callId = new AtomicInteger(1)
            parseCalls(parentCallId, callId, calls).map { v =>
              val callGas = v.gas match {
                case Some(gas) => Some(gas.hexToLong())
                case _         => None
              }
              val callGasUsed = v.gasUsed match {
                case Some(gasUsed) => Some(gasUsed.hexToInt())
                case _             => None
              }
              val callValue = v.value match {
                case Some(value) => Some(value.hexToBigInt().toString)
                case _           => None
              }

              RefinedInternalTransactions(
                "internal_transactions",
                blockNumberPartition,
                blockNumber,
                idx,
                v.callId.get,
                v.parentCallId,
                v.`type`,
                v.from,
                v.to,
                callValue,
                callGas,
                callGasUsed,
                v.input,
                v.output,
                None,
                v.error,
                None
              )
            }
          case _ => List.empty
        }
        List(defaultInternalTransaction) ++ calls
    }
  }
}

case class InternalTransactionContent(
    `type`: String,
    from: Option[String],
    to: Option[String],
    value: Option[String],
    gas: Option[String],
    gasUsed: Option[String],
    input: Option[String],
    output: Option[String],
    time: Option[String],
    calls: Option[List[InternalTransactionCalls]],
    error: Option[String],
    reverted: Option[Reverted])

case class InternalTransactionCalls(
    var callId: Option[Int],
    var parentCallId: Option[Int],
    `type`: String,
    from: Option[String],
    to: Option[String],
    gas: Option[String],
    gasUsed: Option[String],
    input: Option[String],
    output: Option[String],
    value: Option[String],
    error: Option[String],
    calls: Option[List[InternalTransactionCalls]])

case class Reverted(contract: String, message: Option[String])

case class RefinedInternalTransactions(label: String,
                                       bnp: String,
                                       blockNumber: Long,
                                       index: Int,
                                       callId: Int,
                                       parentCallId: Option[Int],
                                       `type`: String,
                                       from: Option[String],
                                       to: Option[String],
                                       value: Option[String],
                                       gas: Option[Long],
                                       gasUsed: Option[Int],
                                       input: Option[String],
                                       output: Option[String],
                                       time: Option[String],
                                       error: Option[String],
                                       reverted: Option[Reverted])

object InternalTransaction {
  def parse(jsonBlock: String): Option[InternalTransaction] = {
    JsonUtil.fromJson[InternalTransaction](jsonBlock)
  }
}
