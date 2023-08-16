package io.klaytn.repository

import io.klaytn.dsl.db.withDB
import io.klaytn.repository.AccountKeyRepository.{AccountKeyDB, AccountKeyTable}

object AccountKeyRepository {
  val AccountKeyDB: String = "finder0101"
  val AccountKeyTable = "account_keys"
}

case class AccountKey(blockNumber: Long,
                      transactionHash: String,
                      accountAddress: String,
                      accountKey: String)

class AccountKeyRepository extends AbstractRepository {
  def insert(accountKey: AccountKey): Unit = {
    withDB(AccountKeyDB) { c =>
      val pstmt = c.prepareStatement(
        s"INSERT IGNORE INTO $AccountKeyTable (`block_number`,`transaction_hash`,`account_address`,`account_key`)" +
          " VALUES (?,?,?,?)")

      pstmt.setLong(1, accountKey.blockNumber)
      pstmt.setString(2, accountKey.transactionHash)
      pstmt.setString(3, accountKey.accountAddress)
      pstmt.setString(4, accountKey.accountKey)

      pstmt.execute()
      pstmt.close()
    }
  }
}
