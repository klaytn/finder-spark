package io.klaytn.apps.restore.tokenURI

import io.klaytn.model.ChainPhase
import io.klaytn.persistent._
import io.klaytn.service._

object TokenURIDeps {
  val chainPhase = ChainPhase.get()

  private val accountPersistentAPI = AccountPersistentAPI.of(chainPhase)
  private val contractPersistentAPI = ContractPersistentAPI.of(chainPhase)
  private val transactionPersistentAPI = TransactionPersistentAPI.of(chainPhase)
  private val transferPersistentAPI = TransferPersistentAPI.of(chainPhase)
  private val eventLogPersistentAPI = EventLogPersistentAPI.of(chainPhase)
  private val blockPersistentAPI = BlockPersistentAPI.of(chainPhase)
  private val internalTransactionPersistentAPI =
    InternalTransactionPersistentAPI.of(chainPhase)

  val holderPersistentAPI =
    HolderPersistentAPI.of(chainPhase, contractPersistentAPI)

  private val caverContractService = CaverContractService.of()
  private val caverService = CaverService.of()

  val contractService = new ContractService(
    contractPersistentAPI,
    accountPersistentAPI,
    transactionPersistentAPI,
    holderPersistentAPI,
    internalTransactionPersistentAPI,
    caverContractService,
    caverService
  )

  private val loadDataInfileService = LoadDataInfileService.of(chainPhase)

  private val nftItemPersistentAPI = NFTItemPersistentAPI.of(chainPhase)
  private val nftItemService =
    new NFTItemService(nftItemPersistentAPI, contractService)

  private val transferService = new TransferService(transferPersistentAPI,
                                                    contractService,
                                                    contractPersistentAPI,
                                                    caverContractService,
                                                    holderPersistentAPI)

  val holderService =
    new HolderService(holderPersistentAPI,
                      transferPersistentAPI,
                      contractService,
                      nftItemService,
                      transferService)
  val blockService = new BlockService(
    blockPersistentAPI,
    transactionPersistentAPI,
    eventLogPersistentAPI,
    caverService,
    loadDataInfileService
  )
}
