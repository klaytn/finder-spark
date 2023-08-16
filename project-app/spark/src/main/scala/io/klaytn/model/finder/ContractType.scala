package io.klaytn.model.finder

object ContractType extends Enumeration {
  type ContractType

  val ERC20 = Value(0)
  val KIP7 = Value(1)
  val KIP17 = Value(2)
  val KIP37 = Value(3)
  val ERC721 = Value(4)
  val ERC1155 = Value(5)
  val ConsensusNode = Value(126)
  val Custom = Value(127)
  val Unknown = Value(-1)

  def from(code: Int): ContractType.Value = {
    code match {
      case 0   => ContractType.ERC20
      case 1   => ContractType.KIP7
      case 2   => ContractType.KIP17
      case 3   => ContractType.KIP37
      case 4   => ContractType.ERC721
      case 5   => ContractType.ERC1155
      case 126 => ContractType.ConsensusNode
      case 127 => ContractType.Custom
      case _   => ContractType.Unknown
    }
  }
}
