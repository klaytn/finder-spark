package io.klaytn.repository

import io.klaytn.dsl.db.withDB
import io.klaytn.model.{Chain, ChainPhase, RefinedTransactionReceipt}
import io.klaytn.utils.JsonUtil
import io.klaytn.utils.JsonUtil.Implicits._

object TransactionRepository {
  val TransactionDB: String = "finder0101"
  val TransactionTable: String = ChainPhase.get().chain match {
    case Chain.baobab | Chain.cypress =>
      "transactions"
  }
}

abstract class TransactionRepository extends AbstractRepository {
  import TransactionRepository._

  def getTransactionHashAndTsAndTxErrorFrom(
      blockNumber: Long,
      transactionIndex: Int): Option[(String, Int, Int, String)] = {
    withDB(TransactionDB) { c =>
      val pstmt = c.prepareStatement(
        s"SELECT `transaction_hash`,`timestamp`,`tx_error`,`from` FROM $TransactionTable WHERE `block_number`=? AND `transaction_index`=?")

      pstmt.setLong(1, blockNumber)
      pstmt.setInt(2, transactionIndex)

      val rs = pstmt.executeQuery()
      val result = if (rs.next()) {
        Some((rs.getString(1), rs.getInt(2), rs.getInt(3), rs.getString(4)))
      } else {
        None
      }

      rs.close()
      pstmt.close()

      result
    }
  }

  def insertTransactionReceipts(
      transactionReceipts: List[RefinedTransactionReceipt]): Unit = {
    withDB(TransactionDB) { c =>
      val pstmt = c.prepareStatement(
        s"INSERT IGNORE INTO $TransactionTable (`block_hash`,`block_number`,`code_format`,`contract_address`,`fee_payer`," +
          "`fee_payer_signatures`,`fee_ratio`,`from`,`gas`,`gas_price`,`gas_used`,`human_readable`,`input`,`key`," +
          "`logs_bloom`,`nft_transfer_count`,`nonce`,`sender_tx_hash`,`signatures`,`status`,`timestamp`,`to`," +
          "`token_transfer_count`,`transaction_hash`,`transaction_index`,`tx_error`,`type`,`type_int`,`value`," +
          "`access_list`,`chain_id`,`max_fee_per_gas`,`max_priority_fee_per_gas`,`effective_gas_price`)" +
          " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
      )

      transactionReceipts.zipWithIndex.foreach {
        case (transactionReceipt, index) =>
          pstmt.setString(1, transactionReceipt.blockHash)
          pstmt.setLong(2, transactionReceipt.blockNumber)
          pstmt.setString(3, transactionReceipt.codeFormat.orNull)
          pstmt.setString(4, transactionReceipt.contractAddress.orNull)
          pstmt.setString(5, transactionReceipt.feePayer.orNull)
          pstmt.setString(6, transactionReceipt.feePayerSignatures match {
            case Some(feePayerSignatures) => JsonUtil.asJson(feePayerSignatures)
            case _                        => null
          })
          pstmt.setString(7, transactionReceipt.feeRatio.orNull)
          pstmt.setString(8, transactionReceipt.from)
          pstmt.setLong(9, transactionReceipt.gas)
          pstmt.setString(10, transactionReceipt.gasPrice)
          pstmt.setInt(11, transactionReceipt.gasUsed)
          transactionReceipt.humanReadable match {
            case Some(humanReadable) =>
              pstmt.setInt(12, if (humanReadable) 1 else 0)
            case _ => pstmt.setNull(12, java.sql.Types.INTEGER)
          }
          pstmt.setString(13, transactionReceipt.input.orNull)
          pstmt.setString(14, transactionReceipt.key.orNull)
          pstmt.setString(15, transactionReceipt.logsBloom)
          pstmt.setInt(16, transactionReceipt.nftTransferCount)
          pstmt.setLong(17, transactionReceipt.nonce)
          pstmt.setString(18, transactionReceipt.senderTxHash)
          pstmt.setString(19, JsonUtil.asJson(transactionReceipt.signatures))
          pstmt.setBoolean(20, transactionReceipt.status)
          pstmt.setInt(21, transactionReceipt.timestamp)
          pstmt.setString(22, transactionReceipt.to.orNull)
          pstmt.setInt(23, transactionReceipt.tokenTransferCount)
          pstmt.setString(24, transactionReceipt.transactionHash)
          pstmt.setInt(25, transactionReceipt.transactionIndex)
          transactionReceipt.txError match {
            case Some(txError) => pstmt.setInt(26, txError)
            case _             => pstmt.setNull(26, java.sql.Types.INTEGER)
          }
          pstmt.setString(27, transactionReceipt.`type`)
          pstmt.setInt(28, transactionReceipt.typeInt)
          pstmt.setString(29, transactionReceipt.value.orNull)
          transactionReceipt.accessList match {
            case Some(accessList) =>
              pstmt.setString(30, JsonUtil.asJson(accessList))
            case _ => pstmt.setNull(30, java.sql.Types.VARCHAR)
          }
          pstmt.setString(31, transactionReceipt.chainId.orNull)
          pstmt.setString(32, transactionReceipt.maxFeePerGas.orNull)
          pstmt.setString(33, transactionReceipt.maxPriorityFeePerGas.orNull)
          pstmt.setString(34, transactionReceipt.effectiveGasPrice.orNull)

          pstmt.addBatch()
          pstmt.clearParameters()

          if ((index + 1) % 3000 == 0) {
            execute(pstmt)
          }
      }

      execute(pstmt)
      pstmt.close()
    }
  }
}
