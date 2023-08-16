package io.klaytn.utils.klaytn

import com.klaytn.caver.abi.ABI
import io.klaytn.utils.klaytn.NumberConverter._

import java.util

object WithdrawDataReadUtil {
  def getAddressWithAddressUint256Type(data: String,
                                       topics: Seq[String]): Option[String] = {
    Option(
      topics.length match {
        case 1 =>
          val result =
            ABI.decodeParameters(util.Arrays.asList("address", "uint256"), data)
          result.get(0).getValue.toString
        case 2 => topics(1).normalizeToAddress()
        case 3 => topics(1).normalizeToAddress()
        case _ => null
      }
    )
  }

  // Not done yet
//  def getAddressWithAddressUint256Uint256Type(data: String, topics: Seq[String]): Option[String] = {
//    Option(
//      topics.length match {
//        case 1 =>
//          val result = ABI.decodeParameters(util.Arrays.asList("address", "uint256", "uint256"), data)
//          result.get(0).getValue.toString
//        case 2 => topics(1).normalizeToAddress()
//        case 3 => topics(1)
//        case _ => null
//      }
//    )
//  }
}
