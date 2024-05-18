package io.klaytn.apps.restore.eventLog

import io.klaytn.model.ChainPhase
import io.klaytn.persistent._
import io.klaytn.service._
import io.klaytn.repository.{AccountKeyRepository, KlaytnNameServiceRepository}

object EventLogRestoreDeps {
  val chainPhase = ChainPhase.get()

  val accountPersistentAPI = AccountPersistentAPI.of(chainPhase)
  val contractPersistentAPI = ContractPersistentAPI.of(chainPhase)
  val transactionPersistentAPI = TransactionPersistentAPI.of(chainPhase)
  val transferPersistentAPI = TransferPersistentAPI.of(chainPhase)
  val eventLogPersistentAPI = EventLogPersistentAPI.of(chainPhase)
  val blockPersistentAPI = BlockPersistentAPI.of(chainPhase)
  val internalTransactionPersistentAPI =
    InternalTransactionPersistentAPI.of(chainPhase)

  val holderPersistentAPI =
    HolderPersistentAPI.of(chainPhase, contractPersistentAPI)

  private val caverContractService = CaverContractService.of()
  val caverService = CaverService.of()

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

  private val transferService = new TransferService(
    transferPersistentAPI,
    contractService,
    contractPersistentAPI,
    caverContractService,
    holderPersistentAPI
  )

  val holderService =
    new HolderService(
      holderPersistentAPI,
      transferPersistentAPI,
      contractService,
      nftItemService,
      transferService
    )
  val blockService = new BlockService(
    blockPersistentAPI,
    transactionPersistentAPI,
    eventLogPersistentAPI,
    caverService,
    loadDataInfileService
  )

  private val klaytnNameServiceRepository = new KlaytnNameServiceRepository()
  private val klaytnNameService = new KlaytnNameServiceService(
    klaytnNameServiceRepository
  )
  private val accountKeyRepository = new AccountKeyRepository()
  private val accountKeyService =
    new AccountKeyService(accountKeyRepository, caverService.getCaver)

  val accountService = new AccountService(
    accountPersistentAPI,
    contractService,
    klaytnNameService,
    caverContractService,
    caverService,
    accountKeyService
  )
}
