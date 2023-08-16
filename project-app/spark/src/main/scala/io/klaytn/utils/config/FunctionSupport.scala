package io.klaytn.utils.config

import io.klaytn.model.ChainPhase

object FunctionSupport {
  def burn(chainPhase: ChainPhase): Boolean = true

  def burnFee(chainPhase: ChainPhase): Boolean = {
    // only for cypress, baobab
    Set(ChainPhase.`prod-cypress`, ChainPhase.`prod-baobab`)
      .contains(chainPhase)
  }

  def nftItems(chainPhase: ChainPhase): Boolean = {
    // only for cypress, baobab
    Set(ChainPhase.`prod-cypress`, ChainPhase.`prod-baobab`)
      .contains(chainPhase)
  }

  def approve(chainPhase: ChainPhase): Boolean = {
    // only for cypress, baobab
    Set(ChainPhase.`prod-cypress`, ChainPhase.`prod-baobab`)
      .contains(chainPhase)
  }

  def blockReward(chainPhase: ChainPhase): Boolean = {
    // only for cypress, baobab
    Set(ChainPhase.`prod-cypress`, ChainPhase.`prod-baobab`)
      .contains(chainPhase)
  }

  def longValueAvgTX(chainPhase: ChainPhase): Boolean = {
    // only for cypress, baobab
    Set(ChainPhase.`prod-cypress`, ChainPhase.`prod-baobab`)
      .contains(chainPhase)
  }
}
