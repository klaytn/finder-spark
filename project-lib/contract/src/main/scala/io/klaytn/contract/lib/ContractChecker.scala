package io.klaytn.contract.lib

import com.klaytn.caver.Caver
import com.klaytn.caver.contract.Contract

import scala.util.Try

class ContractChecker(private val caver: Caver) {
  private val parser = new ByteCodeParser()

  private val interfaceIds = ContractType.values
    .filter(_.interfaceId.nonEmpty)
    .map { contractType =>
      (contractType, contractType.interfaceId)
    }
    .toMap

  private val contractSignatures = ContractType.values.map { contractType =>
    (contractType,
     contractType.signatures.map(x => s"0x${x._1.substring(0, 8)}").toSet)
  }.toMap

  def check(contractAddress: String,
            byteCodes: String): Option[ContractType.Value] = {
    checkByByteCodes(byteCodes) match {
      case Some(contractType) => Some(contractType)
      case _                  => checkByContractAddress(contractAddress)
    }
  }

  def checkByByteCodes(byteCodes: String): Option[ContractType.Value] = {
    val signatures = parser.extractFunctionSignatures(byteCodes)
    contractSignatures.find(_._2.subsetOf(signatures)).map(_._1)
  }

  def checkByContractAddress(
      contractAddress: String): Option[ContractType.Value] = {
    val kip13 = new Contract(caver, Constants.KIP13ABI, contractAddress)

    interfaceIds
      .find { x =>
        Try({
          val interfaces = kip13.call("supportsInterface", x._2)
          interfaces.get(0).getValue.asInstanceOf[Boolean]
        }).getOrElse(false)
      }
      .map(_._1)
  }
}
