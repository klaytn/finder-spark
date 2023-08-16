package io.klaytn.apps.restore.holder

import io.klaytn.model.ChainPhase
import io.klaytn.persistent.{ContractPersistentAPI, HolderPersistentAPI}
import io.klaytn.service.MinusHolderService

object MinusHolderBatchDeps {
  private val chainPhase = ChainPhase.get()
  private val contractPersistentAPI = ContractPersistentAPI.of(chainPhase)
  private val holderPersistentAPI =
    HolderPersistentAPI.of(chainPhase, contractPersistentAPI)

  val service = new MinusHolderService(holderPersistentAPI)
}
