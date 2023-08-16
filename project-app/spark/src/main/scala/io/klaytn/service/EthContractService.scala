package io.klaytn.service

import com.klaytn.caver.abi.ABI
import io.klaytn.utils.config.Constants
import io.klaytn.utils.klaytn.NumberConverter.StringConverter
import org.web3j.abi.datatypes.generated.{Bytes4, Uint256}
import org.web3j.abi.datatypes.{Bool, Type, Utf8String, Function => EthFunction}
import org.web3j.abi.{FunctionEncoder, TypeReference}
import org.web3j.protocol.Web3j
import org.web3j.protocol.core.DefaultBlockParameterName
import org.web3j.protocol.core.methods.request.Transaction

import scala.util.Try

class EthContractService(web3j: Web3j) {
  def getStringValue(method: String, contractAddress: String): String = {
    val inputParameters = java.util.Arrays.asList[Type[_]]()
    val outputParameters = java.util.Arrays
      .asList[TypeReference[_]](new TypeReference[Utf8String]() {})
    val function = new EthFunction(method, inputParameters, outputParameters)

    val response =
      web3j
        .ethCall(
          Transaction.createEthCallTransaction(
            Constants.ZeroAddress,
            contractAddress,
            FunctionEncoder.encode(function)),
          DefaultBlockParameterName.LATEST
        )
        .send()
        .getValue

    ABI
      .decodeParameters(java.util.Arrays.asList("string"), response)
      .get(0)
      .getValue
      .toString
  }

  def getBigIntValue(method: String, contractAddress: String): BigInt = {
    val inputParameters = java.util.Arrays.asList[Type[_]]()
    val outputParameters =
      java.util.Arrays.asList[TypeReference[_]](new TypeReference[Uint256]() {})
    val function = new EthFunction(method, inputParameters, outputParameters)

    val response =
      web3j
        .ethCall(
          Transaction.createEthCallTransaction(
            Constants.ZeroAddress,
            contractAddress,
            FunctionEncoder.encode(function)),
          DefaultBlockParameterName.LATEST
        )
        .send()
        .getValue

    BigInt(
      ABI
        .decodeParameters(java.util.Arrays.asList("uint256"), response)
        .get(0)
        .getValue
        .toString)
  }

  def name(contractAddress: String): Option[String] =
    Try(getStringValue("name", contractAddress)).toOption

  def symbol(contractAddress: String): Option[String] =
    Try(getStringValue("symbol", contractAddress)).toOption

  def decimals(contractAddress: String): Option[Int] =
    Try(getBigIntValue("decimals", contractAddress).toInt).toOption

  def totalSupply(contractAddress: String): Option[BigInt] =
    Try(getBigIntValue("totalSupply", contractAddress)).toOption

  def supportsInterface(contractAddress: String,
                        interfaceId: String): Boolean = {
    val byteInterfaceId = BigInt(interfaceId.hexToBigInt().toInt).toByteArray
    val inputParameters =
      java.util.Arrays.asList[Type[_]](new Bytes4(byteInterfaceId))
    val outputParameters =
      java.util.Arrays.asList[TypeReference[_]](new TypeReference[Bool]() {})
    val function =
      new EthFunction("supportsInterface", inputParameters, outputParameters)

    val response =
      web3j
        .ethCall(
          Transaction.createEthCallTransaction(
            Constants.ZeroAddress,
            contractAddress,
            FunctionEncoder.encode(function)),
          DefaultBlockParameterName.LATEST
        )
        .send()
        .getValue

    Try(
      ABI
        .decodeParameters(java.util.Arrays.asList("bool"), response)
        .get(0)
        .getValue
        .toString
        .toBoolean)
      .getOrElse(false)
  }

  def isERC721(contractAddress: String): Boolean =
    supportsInterface(contractAddress, "0x80ac58cd")

  def isERC1155(contractAddress: String): Boolean =
    supportsInterface(contractAddress, "0xd9b67a26")
}
