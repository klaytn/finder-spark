package io.klaytn.apps.restore.holder

import io.klaytn.model.ChainPhase
import io.klaytn.persistent._
import io.klaytn.service.{
  CaverContractService,
  CaverService,
  ContractService,
  HolderService,
  NFTItemService,
  TransferService
}

object HolderBatchDeps {
  private val chainPhase = ChainPhase.get()

  private val accountPersistentAPI = AccountPersistentAPI.of(chainPhase)
  private val contractPersistentAPI = ContractPersistentAPI.of(chainPhase)
  private val transactionPersistentAPI = TransactionPersistentAPI.of(chainPhase)
  private val transferPersistentAPI = TransferPersistentAPI.of(chainPhase)
  private val internalTransactionPersistentAPI =
    InternalTransactionPersistentAPI.of(chainPhase)
  private val caverContractService = CaverContractService.of()
  private val caverService = CaverService.of()

  val holderPersistentAPI: HolderPersistentAPI =
    HolderPersistentAPI.of(chainPhase, contractPersistentAPI)
  val contractService = new ContractService(
    contractPersistentAPI,
    accountPersistentAPI,
    transactionPersistentAPI,
    holderPersistentAPI,
    internalTransactionPersistentAPI,
    caverContractService,
    caverService
  )

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
}
