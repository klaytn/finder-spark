package io.klaytn.contract.lib

import com.klaytn.caver.Caver
import com.klaytn.caver.abi.ABI
import com.klaytn.caver.kct.kip7.KIP7ConstantData

import java.math.BigInteger
import java.util
import scala.collection.convert.ImplicitConversions._
import scala.math.BigInt.javaBigInteger2bigInt
import scala.util.Try

class KIP7MetadataReader(caver: Caver)
    extends ContractMetadataReader(caver, KIP7ConstantData.ABI) {
  override def supports(): ContractType.Value = ContractType.KIP7

  override def read(contractAddress: String): KIP7 = {
    val kip7 = caver.kct.kip7.create(contractAddress)
    if (kip7.detectInterface().getOrDefault("IKIP7", false).booleanValue()) {
      KIP7(contractAddress,
           symbol(contractAddress),
           name(contractAddress),
           decimals(contractAddress),
           totalSupply(contractAddress))
    } else {
      throw new RuntimeException("This contract is not support KIP-7.")
    }
  }

  def symbol(contractAddress: String): Option[String] = {
    setContractAddress(contractAddress)
    Try(contract.call("symbol").get(0).getValue.asInstanceOf[String]).toOption
  }

  def symbol(contractAddress: String, blockNumber: Long): Option[String] = {
    try {
      val data = call(contractAddress,
                      ABI.encodeFunctionCall("symbol()", Seq(), Seq()),
                      blockNumber)
      Option(
        ABI
          .decodeParameters(util.Arrays.asList("string"), data)
          .get(0)
          .getValue
          .toString)
    } catch {
      case _: Throwable => None
    }
  }

  def name(contractAddress: String): Option[String] = {
    setContractAddress(contractAddress)
    Try(contract.call("name").get(0).getValue.asInstanceOf[String]).toOption
  }

  def name(contractAddress: String, blockNumber: Long): Option[String] = {
    try {
      val data = call(contractAddress,
                      ABI.encodeFunctionCall("name()", Seq(), Seq()),
                      blockNumber)
      Option(
        ABI
          .decodeParameters(util.Arrays.asList("string"), data)
          .get(0)
          .getValue
          .toString)
    } catch {
      case _: Throwable => None
    }
  }

  def decimals(contractAddress: String): Option[Int] = {
    setContractAddress(contractAddress)
    Try(
      contract
        .call("decimals")
        .get(0)
        .getValue
        .asInstanceOf[BigInteger]
        .toInt).toOption
  }

  def totalSupply(contractAddress: String): Option[BigInt] = {
    setContractAddress(contractAddress)
    Option(
      BigInt(
        contract.call("totalSupply").get(0).getValue.asInstanceOf[BigInteger]))
  }

  def balanceOf(contractAddress: String,
                ownerAddress: String): Option[BigInt] = {
    setContractAddress(contractAddress)
    Option(
      BigInt(
        contract
          .call("balanceOf", ownerAddress)
          .get(0)
          .getValue
          .asInstanceOf[BigInteger]))
  }

  def balanceOf(contractAddress: String,
                ownerAddress: String,
                blockNumber: Long): Option[BigInt] = {
    val result = call(contractAddress,
                      ABI.encodeFunctionCall("balanceOf(address)",
                                             Seq("address"),
                                             Seq(ownerAddress)),
                      blockNumber)

    if (result == null) None
    else Some(BigInt(org.web3j.utils.Numeric.toBigInt(result)))
  }
}
