package io.klaytn.model

import com.fasterxml.jackson.module.scala.JsonScalaEnumeration
import io.klaytn.model.finder.{
  AccountKeyType,
  AccountKeyTypeTR,
  KlaytnAccountKeyRoleType
}

sealed abstract class KlaytnAccountKey {
  @JsonScalaEnumeration(classOf[AccountKeyTypeTR])
  def getType: AccountKeyType.Value
}

case class KlaytnAccountKeyLegacy() extends KlaytnAccountKey {
  def getType: AccountKeyType.Value = AccountKeyType.AccountKeyLegacy
}
case class KlaytnAccountKeyFail() extends KlaytnAccountKey {
  def getType: AccountKeyType.Value = AccountKeyType.AccountKeyFail
}
case class KlaytnAccountKeyPublic(publicKey: String) extends KlaytnAccountKey {
  def getType: AccountKeyType.Value = AccountKeyType.AccountKeyPublic
}

case class KlaytnAccountKeyWeightedKey(weight: Long, publicKey: String)
case class KlaytnAccountKeyWeightedMultiSig(
    threshold: Long,
    weightedPublicKeys: Seq[KlaytnAccountKeyWeightedKey])
    extends KlaytnAccountKey {
  def getType: AccountKeyType.Value = AccountKeyType.AccountKeyWeightedMultiSig
}
case class KlaytnAccountKeyRoleBased(
    roles: Map[KlaytnAccountKeyRoleType.Value, KlaytnAccountKey])
    extends KlaytnAccountKey {
  def getType: AccountKeyType.Value = AccountKeyType.AccountKeyRoleBased
}
