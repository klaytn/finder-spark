package io.klaytn.apps.restore.bulkload

import com.typesafe.config.ConfigFactory
import io.klaytn.model.{Chain, ChainPhase}
import io.klaytn.utils.SlackUtil
import io.klaytn.utils.spark.UserConfig

trait BulkLoadHelper {
  private val config = ConfigFactory.load()

  def kafkaLogDirPrefix(): String = {
    val chainPhase = ChainPhase.get()
    val chain = chainPhase.chain
    val phase = chainPhase.phase
    chain match {
      case Chain.cypress =>
        s"klaytn-$phase-lake/klaytn/cypress/label=kafka_log"

      case Chain.baobab =>
        s"klaytn-$phase-lake/klaytn/baobab/label=kafka_log"

      case unknown =>
        throw new UnsupportedOperationException(s"unknown chainPhase: $unknown")
    }
  }

  def outputBucket(): String = UserConfig.baseBucket
  def outputKeyPrefix(): String = s"output/${UserConfig.chainPhase}"
  def outputDirPrefix(): String = s"${outputBucket()}/${outputKeyPrefix()}"

  val start: Int = config.getInt("spark.app.bulk_load.bnp_start")
  val end: Int = config.getInt("spark.app.bulk_load.bnp_end")

  def sendSlackMessage(): Unit = {
    SlackUtil.sendMessage(
      s"""[START] ${this.getClass.getCanonicalName.stripSuffix("$")}
                             |phase: ${UserConfig.chainPhase}
                             |start: $start
                             |end: $end
                             |""".stripMargin)
  }
}
