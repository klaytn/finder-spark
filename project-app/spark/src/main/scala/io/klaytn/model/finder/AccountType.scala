package io.klaytn.model.finder

object AccountType extends Enumeration {
  type AccountType

  val Unknown = Value(-1)
  val EOA = Value(0)
  val SCA = Value(1)
}
