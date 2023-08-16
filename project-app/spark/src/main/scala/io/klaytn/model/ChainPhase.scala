package io.klaytn.model

import com.typesafe.config.{Config, ConfigFactory}

case class ChainPhase(
    phase: Phase.Value,
    chain: Chain.Value,
    finderApiRedisPrefix: String,
) {
  override def toString: String = s"$phase-$chain"
}

//noinspection TypeAnnotation
object ChainPhase {
  val `local-local` = new ChainPhase(Phase.local, Chain.local, "local")
  val `dev-baobab` = new ChainPhase(Phase.dev, Chain.baobab, "baobab")
  val `dev-cypress` = new ChainPhase(Phase.dev, Chain.cypress, "cypress")
  val `prod-baobab` = new ChainPhase(Phase.prod, Chain.baobab, "baobab")
  val `prod-cypress` = new ChainPhase(Phase.prod, Chain.cypress, "cypress")

  private val byTuple: Map[(Phase.Value, Chain.Value), ChainPhase] = Seq(
    `dev-baobab`,
    `dev-cypress`,
    `prod-baobab`,
    `prod-cypress`,
  ).map(x => ((x.phase, x.chain), x)).toMap

  def get(phase: Phase.Value, chain: Chain.Value): ChainPhase = {
    byTuple((phase, chain))
  }

  def get(config: Config): ChainPhase = {
    get(Phase.get(config), Chain.get(config))
  }

  def get(): ChainPhase = {
    get(ConfigFactory.load())
  }
}
