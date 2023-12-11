package io.klaytn.service

import com.github.benmanes.caffeine.cache.{Cache, Caffeine}
import io.klaytn._
import io.klaytn.client.FinderRedis
import io.klaytn.model.finder.{AccountType, Contract, ContractType}
import io.klaytn.model.{Block, RefinedBlock, RefinedEventLog}
import io.klaytn.persistent.AccountPersistentAPI
import io.klaytn.utils._
import io.klaytn.utils.klaytn.NumberConverter._
import io.klaytn.utils.gcs.GCSUtil
import io.klaytn.utils.spark.UserConfig
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.rdd.RDD

import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class AccountService(
    persistentAPI: LazyEval[AccountPersistentAPI],
    contractService: LazyEval[ContractService],
    klaytnNameServiceService: LazyEval[KlaytnNameServiceService],
    caverContractService: LazyEval[CaverContractService],
    caverService: LazyEval[CaverService],
    accountKeyService: LazyEval[AccountKeyService])
    extends Serializable {
  @transient private lazy val accountTypeCache
    : Cache[String, AccountType.Value] = Caffeine
    .newBuilder()
    .expireAfterAccess(60, TimeUnit.SECONDS)
    .maximumSize(100000)
    .build()

  @transient private lazy val consensusNode =
    collection.mutable.Set(persistentAPI.getConsensusNodes(): _*)

  private def getAccountType(address: String): AccountType.Value = {
    accountTypeCache.get(
      address,
      _ => {
        persistentAPI.getAccountType(address)
      }
    )
  }

  private def procSCA(address: String,
                      count: Int,
                      contractFrom: String): Unit = {
    val accountType = getAccountType(address)
    val m = JsonUtil.fromJson[Map[String, String]](contractFrom)

    val contract = if (m.get("tx_error") == "0") {
      caverContractService.getContract(address, m.get("input"))
    } else {
      Contract(address,
               io.klaytn.model.finder.ContractType.Custom,
               None,
               None,
               None,
               None)
    }

    accountType match {
      // EOA: If SCA is entered as EOA in the existing DB: Entered in the wrong order
      // SCA: If a token transfer occurs in the same block, the token is entered first and the contract is registered.
      case AccountType.EOA | AccountType.SCA =>
        persistentAPI
          .updateTotalTXCountAndType(count,
                                     contract.contractType,
                                     m.get("from"),
                                     m.get("txHash"),
                                     address)
      case _ =>
        val (from, txHash) = (Some(m.get("from")), Some(m.get("txHash")))
        val chainCount = caverService.getTransactionCount(address)
        persistentAPI
          .insert(address,
                  AccountType.SCA,
                  chainCount.toInt,
                  contract.contractType,
                  from,
                  txHash,
                  System.currentTimeMillis(),
                  from)
    }

    FinderRedis.del(s"cache/account-by-address::${contract.contractAddress}")
    accountTypeCache.put(address, AccountType.SCA)

    val createdTimestamp = m.get("ts").toInt * 1000L
    contractService.register(contract,
                             createdTimestamp,
                             m.get("tx_error").toInt,
                             0,
                             updateAccount = false)
  }

  private def procEOA(address: String, count: Int): Unit = {
    val accountType = getAccountType(address)
    if (accountType == AccountType.Unknown) {
      try {
        val chainCount = caverService.getTransactionCount(address)
        persistentAPI
          .insert(address,
                  AccountType.EOA,
                  chainCount.toInt,
                  ContractType.Custom,
                  None,
                  None,
                  System.currentTimeMillis(),
                  None)
      } catch {
        case _: Throwable =>
          if (count > 0) persistentAPI.updateTotalTXCount(count, address)
      }
    } else {
      if (count > 0) persistentAPI.updateTotalTXCount(count, address)
    }
  }

  private def procAccount(iter: Iterator[(String, String, Int)]): Unit = {
    val kns = mutable.ArrayBuffer.empty[(String, String)]
    val imp = mutable.ArrayBuffer.empty[(String, String)]

    // iter: [(address, type, tx_count)]
    iter.foreach {
      case (address, typ, count) =>
        if (address.startsWith("KNS:")) {
          kns.append((address, typ))
        } else if (address.startsWith("IMP:")) {
          imp.append((address, typ))
        } else if (typ == "EOA") { // eoa
          procEOA(address, count)
        } else { // sca
          procSCA(address, count, typ)
        }
    }

    kns.foreach {
      case (address0, name0) =>
        val address = address0.replaceFirst("KNS:", "")
        val name1 = name0.replaceFirst("KNS:", "")
        val name = if (name1 == "null" || name1.isEmpty) null else name1

        persistentAPI.updatePrimaryKNS(address, name)
    }

    imp.foreach {
      case (address0, implementationAddress0) =>
        val address = address0.replaceFirst("IMP:", "")
        val implementationAddress =
          implementationAddress0.replaceFirst("IMP:", "")

        contractService.updateImplementationAddress(address,
                                                    implementationAddress)
        persistentAPI.updateContractType(implementationAddress,
                                         ContractType.Custom)
    }
  }

//  private def getCreatedContractFromITX(blockNumber: Long, tx: RefinedTransactionReceipt): Option[(String, String)] = {
//    Utils.retry(3, 500)(caverService.getInternalTransaction(tx.transactionHash)) match {
//      case Some(itxc) =>
//        var toAddress   = ""
//        var fromAddress = ""
//        InternalTransaction(blockNumber, List(itxc))
//          .toRefined()
//          .foreach { i =>
//            if (i.`type` == "CREATE" || i.`type` == "CREATE2") {
//              toAddress = i.to.getOrElse("")
//              fromAddress = i.from.getOrElse("")
//            }
//          }
//        if (toAddress.nonEmpty && fromAddress.nonEmpty) {
//          val m = Map("from" -> fromAddress,
//                      "txHash"   -> tx.transactionHash,
//                      "input"    -> tx.input.getOrElse("-"),
//                      "tx_error" -> tx.txError.getOrElse(0).toString)
//          Some((toAddress, JsonUtil.asJson(m)))
//        } else {
//          None
//        }
//      case _ => None
//    }
//  }

  def updateConsensusNode(block: RefinedBlock): Unit = {
    block.proposer match {
      case Some(proposer) =>
        if (!consensusNode.contains(proposer)) {
          persistentAPI.updateContractType(proposer, ContractType.ConsensusNode)
          consensusNode.add(proposer)
        }
      case _ =>
    }
  }

  def kns(refinedEventLogs: Seq[RefinedEventLog]): Seq[(String, String)] = {
    try {
      klaytnNameServiceService.procAddressAndGetPrimaryKNS(refinedEventLogs)
//        .foreach {
//        case (resolved, name) =>
//          persistentAPI.updatePrimaryKNS(resolved, name)
//      }
    } catch {
      case e: Throwable =>
        if (new Random().nextInt(10) == 1) {
          SlackUtil.sendMessage(s"""${this.getClass.getSimpleName} Error
                                   |error: ${e.getLocalizedMessage}
                                   |stacktrace: ${StringUtils.abbreviate(
                                     ExceptionUtils.getMessage(e),
                                     500)}
                                   |""".stripMargin)
        }
        Seq.empty
    }
  }

  def processWithoutSave(blocks: RDD[Block],
                         numPartitions: Int): RDD[(String, String, Int)] = {
    blocks
      .flatMap { block =>
        val refinedData = block.toRefined
        val (refinedBlock, refinedTransactionReceipts, refinedEventLogs) =
          (refinedData._1, refinedData._2, refinedData._3)

        val txStatusOkRefinedEventLogs =
          refinedEventLogs.filter(_.transactionStatus)

        val result = mutable.Map.empty[String, ArrayBuffer[String]]

        updateConsensusNode(refinedBlock)
        accountKeyService.insertUpdateAccountKey(refinedTransactionReceipts)
        contractService.procChangeTotalSupply(txStatusOkRefinedEventLogs)
        kns(txStatusOkRefinedEventLogs).distinct.foreach {
          case (resolved, name) =>
            result
              .getOrElseUpdate(resolved, ArrayBuffer.empty[String])
              .append(s"KNS:$name")
        }

        refinedTransactionReceipts.foreach { tx =>
          result
            .getOrElseUpdate(tx.from, ArrayBuffer.empty[String])
            .append("EOA")
          result(tx.from).append("TXCNT")
          if (tx.to.isDefined) {
            result
              .getOrElseUpdate(tx.to.get, ArrayBuffer.empty[String])
              .append("EOA")
          }

          if (tx.contractAddress.isDefined) {
            val json = JsonUtil.asJson(Map(
              "from" -> tx.from,
              "txHash" -> tx.transactionHash,
              "input" -> tx.input.getOrElse("-"),
              "ts" -> tx.timestamp.toString,
              "tx_error" -> tx.txError.getOrElse(0).toString
            ))
            result
              .getOrElseUpdate(tx.contractAddress.get,
                               ArrayBuffer.empty[String])
              .append(json)
          } /*else if (tx.isCreate) {
                try {
                  val data = getCreatedContractFromITX(block.blockNumber, tx)
                  if (data.isDefined) {
                    result.getOrElseUpdate(data.get._1, ArrayBuffer.empty[String]).append(data.get._2)
                  }
                } catch {
                  case e: Throwable =>
                    SlackUtil.sendMessage(s"""#get internal transaction error: ${tx.transactionHash}
                                             |${e.getMessage}
                                             |${StringUtils.abbreviate(ExceptionUtils.getStackTrace(ex), 500)}
                                             |""".stripMargin)
                }
              }*/
        }

        txStatusOkRefinedEventLogs.filter(_.isTransferEvent).foreach { e =>
          val (_, from, to, _, _) = e.extractTransferInfo()
          result.getOrElseUpdate(from, ArrayBuffer.empty[String]).append("EOA")
          result.getOrElseUpdate(to, ArrayBuffer.empty[String]).append("EOA")
        }

        txStatusOkRefinedEventLogs.filter(_.isUpgraded).foreach { e =>
          try {
            val implementationAddress =
              if (e.topics.length == 2) e.topics(1).normalizeToAddress()
              else e.data.normalizeToAddress()

            result
              .getOrElseUpdate(e.address, ArrayBuffer.empty[String])
              .append(s"IMP:$implementationAddress")
          } catch {
            case ex: Throwable =>
              GCSUtil.writeText(UserConfig.baseBucket,
                                s"output/imp_addr/${e.blockNumber}.except",
                                ex.getStackTrace.mkString("\n"))
          }
        }

        result.toSeq
      }
      .groupByKey(numPartitions)
      .mapPartitions { iter =>
        val result = mutable.Set.empty[(String, String, Int)]
        // iter: [(address, ["EOA", "EOA", "KNS:klay.klay", JsonMap("from"->"address", "txHash"->"hash", "input"->"input or -")])]
        iter.foreach {
          case (address, _types) =>
            val types = _types.toSeq.flatten

            val txCount = types.count(_ == "TXCNT")
            val contractFrom =
              types.filter(
                x =>
                  x != "EOA" && x != "TXCNT" && !x.startsWith("KNS:") && !x
                    .startsWith("IMP:"))

            val implementationAddressList = types.filter(_.startsWith("IMP:"))

            if (contractFrom.nonEmpty) {
              result.add((address, contractFrom.head, txCount))
            } else if (implementationAddressList.isEmpty) {
              result.add((address, "EOA", txCount))
            }

            types.filter(_.startsWith("KNS:")).foreach { kns =>
              result.add((s"KNS:$address", kns, 0))
            }

            implementationAddressList.foreach { implementationAddress =>
              result.add((s"IMP:$address", implementationAddress, 0))
            }
        }
        result.toSeq.iterator
      }
  }

  def process(blocks: RDD[Block], numPartitions: Int): Unit = {
    processWithoutSave(blocks, numPartitions)
      .foreachPartition(procAccount)
  }
}
