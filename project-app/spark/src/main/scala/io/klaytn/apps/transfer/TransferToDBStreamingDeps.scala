package io.klaytn.apps.transfer

import io.klaytn.model.ChainPhase
import io.klaytn.persistent._
import io.klaytn.service.{
  CaverContractService,
  CaverService,
  ContractService,
  TransferService
}

object TransferToDBStreamingDeps {
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
  val service = new TransferService(
    transferPersistentAPI,
    contractService,
    contractPersistentAPI,
    caverContractService,
    holderPersistentAPI
  )
}
