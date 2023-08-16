package io.klaytn.service

import com.klaytn.caver.abi.ABI
import io.klaytn._
import io.klaytn.model.{ChainPhase, EventLogType, RefinedEventLog}
import io.klaytn.repository.{KlaytnNameService, KlaytnNameServiceRepository}
import io.klaytn.utils.config.Constants
import io.klaytn.utils.klaytn.KlaytnNameServiceUtil
import io.klaytn.utils.spark.UserConfig

import java.util
import scala.collection.mutable

class KlaytnNameServiceService(
    repository: LazyEval[KlaytnNameServiceRepository])
    extends Serializable {
  def procReverse(reverseAddress: String, data: String): (String, String) = { // (resolved, name)
    if (!reverseAddress.contains(KlaytnNameServiceUtil.ReverseSuffix))
      return null

    val resolved = "0x" + reverseAddress.replace(
      KlaytnNameServiceUtil.ReverseSuffix,
      "")
    val name0 = ABI
      .decodeParameters(util.Arrays.asList("string"), data)
      .get(0)
      .getValue
      .toString
    val name = if (name0 == "") null else name0

    (resolved, name)

//    repository.getKlaytnNameServiceByName(name) match {
//      case Some(kns) =>
//        if (kns.resolvedAddress == resolved) (resolved, name)
//        else null
//      case _ => null
//    }
  }

  def procResolve(name: String, nameHash: String): (String, String) = { // (resolved, name)
    if (!name.endsWith(".klay")) return null

    val resolver = KlaytnNameServiceUtil.getResolver(nameHash)
    val resolved = KlaytnNameServiceUtil.getAddress(nameHash, resolver)

    if (resolved != Constants.ZeroAddress) {
      if (repository.getKlaytnNameServiceByNameHash(nameHash).isDefined) {
        repository.updateAddresses(nameHash, resolved, resolver)
        val primaryName = KlaytnNameServiceUtil.getPrimaryName(resolved)
        (resolved, if (primaryName == "") null else primaryName)
      } else {
        repository.insert(
          KlaytnNameService(name,
                            resolved,
                            resolver,
                            nameHash,
                            KlaytnNameServiceUtil.getTokenId(nameHash)))
        null
      }
    } else null
  }

  def procAddressAndGetPrimaryKNS(refinedEventLogs: Seq[RefinedEventLog])
    : Seq[(String, String)] = { // Seq[(resolved, name)]
    // only supports prod cypress.
    if (UserConfig.chainPhase != ChainPhase.`prod-cypress`) {
      return Seq.empty
    }

    val transferFrom = mutable.ArrayBuffer.empty[String]

    val result1 = refinedEventLogs.flatMap { refinedEventLog =>
      val typ = refinedEventLog.getType
      val (nameHash, isReverse) =
        if (typ == EventLogType.AddrChanged || typ == EventLogType.NewResolver) {
          (refinedEventLog.topics(1), false)
        } else if (typ == EventLogType.NTFTransfer && refinedEventLog.address == KlaytnNameServiceUtil.RegistryAddress) {
          // reflect the change in primary in to.
          transferFrom.append(refinedEventLog.topics(1))
          (refinedEventLog.topics(3), false)
        } else if (typ == EventLogType.NameChanged) {
          (refinedEventLog.topics(1), true)
        } else (null, false)

      val result =
        if (nameHash == null) null
        else {
          KlaytnNameServiceUtil.getName(nameHash) match {
            case Some(name) =>
              if (isReverse) procReverse(name, refinedEventLog.data)
              else procResolve(name, nameHash)
            case _ => null
          }
        }

      Option(result)
    }

    val result2 = transferFrom.map { addr =>
      val primaryName = KlaytnNameServiceUtil.getPrimaryName(addr)
      (addr, if (primaryName == "") null else primaryName)
    }

    result1 ++ result2
  }
}
