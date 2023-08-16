package io.klaytn.contract.lib

import com.klaytn.caver.Caver
import com.klaytn.caver.abi.ABI
import com.klaytn.caver.kct.kip37.KIP37ConstantData

import java.math.BigInteger
import scala.collection.convert.ImplicitConversions._
import scala.util.Try

class KIP37MetadataReader(caver: Caver)
    extends ContractMetadataReader(caver, KIP37ConstantData.ABI) {
  override def supports(): ContractType.Value = ContractType.KIP37

  override def read(contractAddress: String): KIP37 = {
    val kip37 = caver.kct.kip37.create(contractAddress)
    if (kip37.detectInterface().getOrDefault("IKIP37", false).booleanValue()) {
      KIP37(contractAddress)
    } else {
      throw new RuntimeException("This contract is not support KIP-37.")
    }
  }

  def uri(contractAddress: String, id: BigInt): Option[String] = {
    setContractAddress(contractAddress)
    Try(
      contract
        .call("uri", id.bigInteger)
        .get(0)
        .getValue
        .asInstanceOf[String]).toOption
  }

  def totalSupply(contractAddress: String, id: BigInt): Option[BigInt] = {
    setContractAddress(contractAddress)
    Option(
      BigInt(
        contract
          .call("totalSupply", id.bigInteger)
          .get(0)
          .getValue
          .asInstanceOf[BigInteger]))
  }

  def balanceOf(contractAddress: String,
                ownerAddress: String,
                id: BigInt): Option[BigInt] = {
    setContractAddress(contractAddress)
    Option(
      BigInt(
        contract
          .call("balanceOf", ownerAddress, id.bigInteger)
          .get(0)
          .getValue
          .asInstanceOf[BigInteger]))
  }

  def balanceOf(contractAddress: String,
                ownerAddress: String,
                id: BigInt,
                blockNumber: Long): Option[BigInt] = {
    val result = call(contractAddress,
                      ABI.encodeFunctionCall("balanceOf(address,uint256)",
                                             Seq("address", "uint256"),
                                             Seq(ownerAddress, id.bigInteger)),
                      blockNumber)

    if (result == null) None
    else Some(BigInt(org.web3j.utils.Numeric.toBigInt(result)))
  }
}
