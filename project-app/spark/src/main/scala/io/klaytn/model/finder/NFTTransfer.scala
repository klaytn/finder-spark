package io.klaytn.model.finder

case class NFTTransfer(id: Option[Long],
                       contractType: ContractType.Value,
                       contractAddress: String,
                       from: String,
                       to: String,
                       tokenCount: String,
                       tokenId: BigInt,
                       timestamp: Int,
                       blockNumber: Long,
                       transactionHash: String,
                       displayOrder: String)
