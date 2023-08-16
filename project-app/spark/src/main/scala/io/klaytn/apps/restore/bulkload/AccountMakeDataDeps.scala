package io.klaytn.apps.restore.bulkload

import io.klaytn.model.ChainPhase
import io.klaytn.persistent._
import io.klaytn.repository.{AccountKeyRepository, KlaytnNameServiceRepository}
import io.klaytn.service._

object AccountMakeDataDeps {
  private val chainPhase = ChainPhase.get()

  private val accountPersistentAPI = AccountPersistentAPI.of(chainPhase)
  private val contractPersistentAPI = ContractPersistentAPI.of(chainPhase)

  private val transactionPersistentAPI = TransactionPersistentAPI.of(chainPhase)
  private val holderPersistentAPI =
    HolderPersistentAPI.of(chainPhase, contractPersistentAPI)
  private val internalTransactionPersistentAPI =
    InternalTransactionPersistentAPI.of(chainPhase)

  private val caverService = CaverService.of()
  val caverContractService: CaverContractService = CaverContractService.of()

  private val contractService = new ContractService(
    contractPersistentAPI,
    accountPersistentAPI,
    transactionPersistentAPI,
    holderPersistentAPI,
    internalTransactionPersistentAPI,
    caverContractService,
    caverService
  )

  private val klaytnNameServiceRepository = new KlaytnNameServiceRepository()
  private val klaytnNameService = new KlaytnNameServiceService(
    klaytnNameServiceRepository)
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
