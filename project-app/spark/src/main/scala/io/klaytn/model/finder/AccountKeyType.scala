package io.klaytn.model.finder

import com.fasterxml.jackson.core.`type`.TypeReference

object AccountKeyType extends Enumeration {
  type AccountKeyType

  val AccountKeyLegacy = Value(1)
  val AccountKeyPublic = Value(2)
  val AccountKeyFail = Value(3)
  val AccountKeyWeightedMultiSig = Value(4)
  val AccountKeyRoleBased = Value(5)
}

class AccountKeyTypeTR extends TypeReference[AccountKeyType.type]
