package io.klaytn.model.finder

case class NFTApprove(id: Option[Long],
                      blockNumber: Long,
                      transactionHash: String,
                      accountAddress: String,
                      spenderAddress: String,
                      contractType: ContractType.Value,
                      contractAddress: String,
                      approvedTokenId: Option[String],
                      approvedAll: Boolean,
                      timestamp: Int)
