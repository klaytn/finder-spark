package io.klaytn.model.finder

import com.klaytn.caver.account.AccountKeyRoleBased

object KlaytnAccountKeyRoleType extends Enumeration {
  type KlaytnAccountKeyRoleType

  val RoleTransaction = Value(
    AccountKeyRoleBased.RoleGroup.TRANSACTION.getIndex)
  val RoleAccountUpdate = Value(
    AccountKeyRoleBased.RoleGroup.ACCOUNT_UPDATE.getIndex)
  val RoleFeePayer = Value(AccountKeyRoleBased.RoleGroup.FEE_PAYER.getIndex)
}
