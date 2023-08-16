package io.klaytn.apps.adhoc.restore

import io.klaytn.model.ChainPhase
import io.klaytn.persistent._
import io.klaytn.service.{
  CaverContractService,
  CaverService,
  ContractService,
  TransferService
}

object RestoreEventLogBatchDeps {
  private val chainPhase = ChainPhase.get()

  private val accountPersistentAPI = AccountPersistentAPI.of(chainPhase)
  private val contractPersistentAPI = ContractPersistentAPI.of(chainPhase)
  private val transactionPersistentAPI = TransactionPersistentAPI.of(chainPhase)
  private val holderPersistentAPI =
    HolderPersistentAPI.of(chainPhase, contractPersistentAPI)
  private val transferPersistentAPI = TransferPersistentAPI.of(chainPhase)
  private val internalTransactionPersistentAPI =
    InternalTransactionPersistentAPI.of(chainPhase)
  private val caverContractService = CaverContractService.of()

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

  val eventLogPersistentAPI: EventLogPersistentAPI =
    EventLogPersistentAPI.of(chainPhase)
  val transferService: TransferService = new TransferService(
    transferPersistentAPI,
    contractService,
    contractPersistentAPI,
    caverContractService,
    holderPersistentAPI
  )
}
