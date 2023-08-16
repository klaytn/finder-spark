package io.klaytn.contract.lib

trait ContractMetadata

case class KIP7(address: String,
                symbol: Option[String],
                name: Option[String],
                decimals: Option[Int],
                totalSupply: Option[BigInt])
    extends ContractMetadata

case class KIP17(address: String,
                 symbol: Option[String],
                 name: Option[String],
                 totalSupply: Option[BigInt])
    extends ContractMetadata

case class KIP37(address: String) extends ContractMetadata

case class ERC20(address: String,
                 symbol: Option[String],
                 name: Option[String],
                 decimals: Option[Int],
                 totalSupply: Option[BigInt])
    extends ContractMetadata

case class ERC721(address: String,
                  symbol: Option[String],
                  name: Option[String],
                  totalSupply: Option[BigInt])
    extends ContractMetadata

case class ERC1155(address: String) extends ContractMetadata
