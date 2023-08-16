package io.klaytn.model.finder

case class Contract(contractAddress: String,
                    contractType: ContractType.Value,
                    name: Option[String],
                    symbol: Option[String],
                    decimal: Option[Int],
                    totalSupply: Option[BigInt]) {}

object Contract {
  def empty(contractAddress: String): Contract = {
    Contract(contractAddress,
             io.klaytn.model.finder.ContractType.Custom,
             None,
             None,
             None,
             None)
  }
}
