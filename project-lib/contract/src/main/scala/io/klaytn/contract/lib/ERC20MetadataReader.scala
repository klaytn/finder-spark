package io.klaytn.contract.lib

import com.klaytn.caver.Caver
import com.klaytn.caver.abi.ABI

import java.math.BigInteger
import scala.collection.convert.ImplicitConversions._
import scala.math.BigInt.javaBigInteger2bigInt
import scala.util.Try

class ERC20MetadataReader(caver: Caver)
    extends ContractMetadataReader(caver, Constants.ERC20ABI) {
  override def supports(): ContractType.Value = ContractType.ERC20

  override def read(contractAddress: String): ERC20 = {
    ERC20(contractAddress,
          symbol(contractAddress),
          name(contractAddress),
          decimals(contractAddress),
          totalSupply(contractAddress))
  }

  def symbol(contractAddress: String): Option[String] = {
    setContractAddress(contractAddress)
    Try(contract.call("symbol").get(0).getValue.asInstanceOf[String]).toOption
  }

  def name(contractAddress: String): Option[String] = {
    setContractAddress(contractAddress)
    Try(contract.call("name").get(0).getValue.asInstanceOf[String]).toOption
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
