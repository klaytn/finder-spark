package io.klaytn.contract.lib

import com.klaytn.caver.Caver
import com.klaytn.caver.abi.ABI
import com.klaytn.caver.kct.kip17.KIP17ConstantData

import java.math.BigInteger
import scala.collection.convert.ImplicitConversions._
import scala.util.Try

class KIP17MetadataReader(caver: Caver)
    extends ContractMetadataReader(caver, KIP17ConstantData.ABI) {
  override def supports(): ContractType.Value = ContractType.KIP17

  override def read(contractAddress: String): KIP17 = {
    val kip17 = caver.kct.kip17.create(contractAddress)
    if (kip17.detectInterface().getOrDefault("IKIP17", false).booleanValue()) {
      KIP17(contractAddress,
            symbol(contractAddress),
            name(contractAddress),
            totalSupply(contractAddress))
    } else {
      throw new RuntimeException("This contract is not support KIP-17.")
    }
  }

  def symbol(contractAddress: String): Option[String] = {
    setContractAddress(contractAddress)
    Try(contract.call("symbol").get(0).getValue.asInstanceOf[String]).toOption
  }

  def name(contractAddress: String): Option[String] = {
    setContractAddress(contractAddress)
    Try(contract.call("name").get(0).getValue.asInstanceOf[String]).toOption
  }

  def totalSupply(contractAddress: String): Option[BigInt] = {
    setContractAddress(contractAddress)
    Try(
      BigInt(
        contract
          .call("totalSupply")
          .get(0)
          .getValue
          .asInstanceOf[BigInteger])).toOption
  }

  def tokenURI(contractAddress: String, tokenId: BigInt): Option[String] = {
    setContractAddress(contractAddress)
    Try(
      contract
        .call("tokenURI", tokenId.bigInteger)
        .get(0)
        .getValue
        .asInstanceOf[String]).toOption
  }

  def tokenURI(contractAddress: String,
               tokenId: BigInt,
               blockNumber: Long): Option[String] = {
    Option(
      call(contractAddress,
           ABI.encodeFunctionCall("tokenURI(uint256)",
                                  Seq("uint256"),
                                  Seq(tokenId.bigInteger)),
           blockNumber))
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

  def ownerOf(contractAddress: String, tokenId: BigInt): Option[String] = {
    setContractAddress(contractAddress)
    Option(
      contract
        .call("ownerOf", tokenId.bigInteger)
        .get(0)
        .getValue
        .asInstanceOf[String])
  }
}
