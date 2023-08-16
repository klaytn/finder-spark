package io.klaytn.utils.klaytn

import com.klaytn.caver.abi.ABI
import com.klaytn.caver.abi.datatypes.generated.Uint256
import io.klaytn.utils.klaytn.NumberConverter._

import java.util
import scala.jdk.CollectionConverters.asScalaBufferConverter

object TransferDataReadUtil {
  def decodeTransferSingleData(data: String): (BigInt, String) = {
    val tokenId = data.substring(2, 66)
    val count = data.substring(67)
    (tokenId.hexToBigInt(), count.hexToBigInt().to64BitsHex())
  }

  def decodeTransferSingleDataV2(
      data: String): (String, String, String, BigInt, String) = {
    // event TransferSingle(address indexed _operator, address indexed _from, address indexed _to, uint256 _id, uint256 _value);
    val s = data.replaceFirst("0x", "").grouped(64).toSeq
    if (s.length != 5) {
      throw new RuntimeException("cannot parse TransferSingle data")
    }
    val (operator, from, to, id, value) =
      (s.head.normalize(true),
       s(1).normalize(true),
       s(2).normalize(true),
       s(3).normalize(true),
       s(4).normalize(true))
    (operator, from, to, id.hexToBigInt(), value.hexToBigInt().to64BitsHex())
  }

  def readTransferBatchIDsAndValues(
      data: String): (Seq[BigInt], Seq[String]) = {
    val result =
      ABI.decodeParameters(util.Arrays.asList("uint256[]", "uint256[]"), data)

    val tokenIds =
      result
        .get(0)
        .getValue
        .asInstanceOf[java.util.ArrayList[Uint256]]
        .asScala
        .map(x => BigInt(x.getValue))
    val values =
      result
        .get(1)
        .getValue
        .asInstanceOf[java.util.ArrayList[Uint256]]
        .asScala
        .map(x => BigInt(x.getValue).to64BitsHex())

    (tokenIds, values)
  }

  def readTransferBatchIDsAndValuesV2(
      data: String): (String, String, String, Seq[BigInt], Seq[String]) = {
    val result =
      ABI.decodeParameters(util.Arrays.asList("address",
                                              "address",
                                              "address",
                                              "uint256[]",
                                              "uint256[]"),
                           data)

    val operator = result.get(0).getValue.toString
    val from = result.get(1).getValue.toString
    val to = result.get(2).getValue.toString

    val tokenIds =
      result
        .get(3)
        .getValue
        .asInstanceOf[java.util.ArrayList[Uint256]]
        .asScala
        .map(x => BigInt(x.getValue))
    val values =
      result
        .get(4)
        .getValue
        .asInstanceOf[java.util.ArrayList[Uint256]]
        .asScala
        .map(x => BigInt(x.getValue).to64BitsHex())

    (operator, from, to, tokenIds, values)
  }
}
