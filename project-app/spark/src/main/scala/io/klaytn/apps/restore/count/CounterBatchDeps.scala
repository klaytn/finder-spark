package io.klaytn.apps.restore.count

import io.klaytn.model.ChainPhase
import io.klaytn.persistent._
import io.klaytn.service._

object CounterBatchDeps {
  private val chainPhase = ChainPhase.get()

  private val transactionPersistentAPI = TransactionPersistentAPI.of(chainPhase)
  private val internalTransactionPersistentAPI =
    InternalTransactionPersistentAPI.of(chainPhase)
  private val transferPersistentAPI = TransferPersistentAPI.of(chainPhase)
  private val caverContractService = CaverContractService.of()

  val accountPersistentAPI: AccountPersistentAPI =
    AccountPersistentAPI.of(chainPhase)
  val contractPersistentAPI: ContractPersistentAPI =
    ContractPersistentAPI.of(chainPhase)
  private val holderPersistentAPI =
    HolderPersistentAPI.of(chainPhase, contractPersistentAPI)

  private val caverService = CaverService.of()

  private val contractService = new ContractService(
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
