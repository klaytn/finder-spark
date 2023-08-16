package io.klaytn.service

import com.klaytn.caver.Caver
import com.klaytn.caver.account._
import io.klaytn._
import io.klaytn.model._
import io.klaytn.model.finder.KlaytnAccountKeyRoleType
import io.klaytn.repository.{AccountKey, AccountKeyRepository}
import io.klaytn.utils.ObjectMapperUtil

import scala.jdk.CollectionConverters.asScalaBufferConverter
import scala.util.Try

class AccountKeyService(accountKeyRepository: LazyEval[AccountKeyRepository],
                        caver: LazyEval[Caver])
    extends Serializable {
  def getKlaytnAccountKey(accountKey: IAccountKey): KlaytnAccountKey = {
    accountKey match {
      case legacy: AccountKeyLegacy => KlaytnAccountKeyLegacy()
      case fail: AccountKeyFail     => KlaytnAccountKeyFail()
      case public: AccountKeyPublic =>
        KlaytnAccountKeyPublic(public.getPublicKey)
      case weightedMultiSig: AccountKeyWeightedMultiSig =>
        KlaytnAccountKeyWeightedMultiSig(
          threshold = weightedMultiSig.getThreshold.longValue(),
          weightedPublicKeys =
            weightedMultiSig.getWeightedPublicKeys.asScala.map {
              weightedPublicKey =>
                KlaytnAccountKeyWeightedKey(
                  weightedPublicKey.getWeight.longValue(),
                  weightedPublicKey.getPublicKey
                )
            }
        )
      case roleBased: AccountKeyRoleBased =>
        KlaytnAccountKeyRoleBased(
          Map(
            KlaytnAccountKeyRoleType.RoleTransaction ->
              Try(getKlaytnAccountKey(roleBased.getRoleTransactionKey))
                .getOrElse(null),
            KlaytnAccountKeyRoleType.RoleAccountUpdate ->
              Try(getKlaytnAccountKey(roleBased.getRoleAccountUpdateKey))
                .getOrElse(null),
            KlaytnAccountKeyRoleType.RoleFeePayer ->
              Try(getKlaytnAccountKey(roleBased.getRoleFeePayerKey))
                .getOrElse(null)
          )
        )
      case _ => null
    }
  }

  def insertUpdateAccountKey(
      refinedTransactionReceipts: Seq[RefinedTransactionReceipt]): Unit = {
    val filteringType =
      Set("TxTypeAccountUpdate",
          "TxTypeFeeDelegatedAccountUpdate",
          "TxTypeFeeDelegatedAccountUpdateWithRatio")
    refinedTransactionReceipts
      .filter(t => filteringType.contains(t.`type`))
      .foreach { t =>
        t.key.foreach { txKey =>
          try {
            val result = caver.rpc.klay
              .decodeAccountKey(txKey)
              .send()
              .getResult
              .getAccountKey
            val key = getKlaytnAccountKey(result)
            val accountKeyJson = ObjectMapperUtil.asJson(key)
            accountKeyRepository.insert(
              AccountKey(t.blockNumber,
                         t.transactionHash,
                         t.from,
                         accountKeyJson))
          } catch { case _: Throwable => }
        }
      }
  }
}
