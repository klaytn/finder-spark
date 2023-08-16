package io.klaytn.model.finder

case class TokenApprove(id: Option[Long],
                        blockNumber: Long,
                        transactionHash: String,
                        accountAddress: String,
                        spenderAddress: String,
                        contractType: ContractType.Value,
                        contractAddress: String,
                        approvedAmount: BigInt,
                        timestamp: Int)
