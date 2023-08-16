package io.klaytn.model.finder.es

import io.klaytn.utils.JsonUtil
import JsonUtil.Implicits._

case class ESAccount(
    address: String,
    `type`: Int,
    balance: Option[BigDecimal], // 36,18
    contract_type: Int,
    contract_creator_address: Option[String],
    contract_creator_tx_hash: Option[String],
    kns_domain: Option[String],
    address_label: Option[String],
    tags: Option[Seq[String]],
    updated_at: Long,
) {
  def asJson: String = JsonUtil.asJson(this)
}
