package io.klaytn.apps.restore.approveBurn

import io.klaytn.model.ChainPhase
import io.klaytn.persistent._
import io.klaytn.persistent.impl.rdb.RDBTransferPersistentAPI
import io.klaytn.service.{
  CaverContractService,
  CaverService,
  TransferService,
  ContractService
}

object ApproveBurnBatchDeps {
  private val chainPhase = ChainPhase.get()

  private val accountPersistentAPI = AccountPersistentAPI.of(chainPhase)
  private val contractPersistentAPI = ContractPersistentAPI.of(chainPhase)
  private val transactionPersistentAPI = TransactionPersistentAPI.of(chainPhase)
  private val holderPersistentAPI =
    HolderPersistentAPI.of(chainPhase, contractPersistentAPI)
  private val internalTransactionPersistentAPI =
    InternalTransactionPersistentAPI.of(chainPhase)
  private val caverContractService = CaverContractService.of()
  private val caverService = CaverService.of()

  val transferPersistentAPI: RDBTransferPersistentAPI =
    TransferPersistentAPI.of(chainPhase)

  private val contractService = new ContractService(
    contractPersistentAPI,
    accountPersistentAPI,
    transactionPersistentAPI,
    holderPersistentAPI,
    internalTransactionPersistentAPI,
    caverContractService,
    caverService
  )

  val transferService = new TransferService(
    transferPersistentAPI,
    contractService,
    contractPersistentAPI,
    caverContractService,
    holderPersistentAPI
  )
}
