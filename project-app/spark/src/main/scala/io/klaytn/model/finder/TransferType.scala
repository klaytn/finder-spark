package io.klaytn.model.finder

object TransferType extends Enumeration {
  type TransferType

  val Unknown = Value(-1)
  val TOKEN = Value(0)
  val NFT = Value(1)
}
