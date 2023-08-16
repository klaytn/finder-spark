package io.klaytn.model.finder

case class TokenTransfer(id: Long,
                         contractAddress: String,
                         from: String,
                         to: String,
                         amount: String,
                         timestamp: Int,
                         blockNumber: Long,
                         transactionHash: String,
                         displayOrder: String)
