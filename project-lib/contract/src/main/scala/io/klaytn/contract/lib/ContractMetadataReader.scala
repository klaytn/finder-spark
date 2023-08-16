package io.klaytn.contract.lib

import com.klaytn.caver.Caver
import com.klaytn.caver.contract.Contract
import com.klaytn.caver.methods.request.CallObject
import org.web3j.protocol.core.DefaultBlockParameterNumber

import java.lang.reflect.Method

abstract class ContractMetadataReader(caver: Caver, abi: String) {
  protected val contract: Contract = Contract.create(caver, abi)

  protected lazy val method: Method = {
    val method =
      contract.getClass.getDeclaredMethod("setContractAddress", classOf[String])
    method.setAccessible(true)
    method
  }

  protected def setContractAddress(contractAddress: String): AnyRef = {
    method.invoke(contract, contractAddress)
  }

  def supports(): ContractType.Value
  def read(contractAddress: String): ContractMetadata

  protected def call(contractAddress: String,
                     callData: String,
                     blockNumber: Long): String = {
    val callObject = CallObject.createCallObject()
    callObject.setTo(contractAddress)
    callObject.setData(callData)

    caver.rpc.klay
      .call(callObject, new DefaultBlockParameterNumber(blockNumber))
      .send()
      .getResult
  }
}
