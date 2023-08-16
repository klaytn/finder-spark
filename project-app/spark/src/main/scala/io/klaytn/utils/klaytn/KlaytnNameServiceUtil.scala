package io.klaytn.utils.klaytn

import com.klaytn.caver.abi.datatypes.Address
import com.klaytn.caver.contract.Contract
import io.klaytn.contract.lib.Constants
import io.klaytn.service.CaverFactory
import io.klaytn.utils.klaytn.NumberConverter._
import org.web3j.ens.NameHash

object KlaytnNameServiceUtil {
  val RegistryAddress = "0x0892ed3424851d2bab4ac1091fa93c9851eb5d7d"
  val PublicResolverAddress = "0x1cf101c4886dcb7d1a8b95801e7e787a42547cbc"

  val DefaultReverseResolver = "0x149e752f92af8694202047d3bb7da95ee1912051"
  val ReverseRecords = "0x87f4483e4157a6592dd1d1546f145b5ee22c790a"
//  val PublicResolverAddress = "0xe2ae210c9b8601e00ede4ae5b9b23a80dcd12e3c"

  val ReverseSuffix = ".addr.reverse"

  def getResolver(nameHash: String): String =
    getResolver(nameHash, RegistryAddress)

  def getResolver(nameHash: String, registryAddress: String): String = {
    val reg = new Contract(CaverFactory.caver,
                           Constants.KNSRegistryABI,
                           registryAddress)
    reg.call("resolver", nameHash).get(0).asInstanceOf[Address].toString
  }

  def getAddressByName(name: String): String = getAddress(getNameHash(name))

  def getAddress(nameHash: String): String =
    getAddress(nameHash, PublicResolverAddress)

  def getAddress(nameHash: String, resolverAddress: String): String = {
    val resolver = new Contract(CaverFactory.caver,
                                Constants.KNSResolverABI,
                                resolverAddress)
    resolver.call("addr", nameHash).get(0).asInstanceOf[Address].toString
  }

  def getTokenId(nameHash: String): BigInt = nameHash.hexToBigInt()

  def getNameHash(name: String): String = NameHash.nameHash(name)

  def getName(nameHash: String): Option[String] = getName(getTokenId(nameHash))

  def getName(tokenId: BigInt): Option[String] = {
    try {
      val kip17 = CaverFactory.caver.kct.kip17.create(RegistryAddress)
      val tokenUri = kip17.tokenURI(tokenId.bigInteger)
      // The current tokenUri contains either .addr.reverse or .klay, so we use that information.
      tokenUri
        .split("""/""")
        .find(x => x.endsWith(ReverseSuffix) || x.endsWith(".klay"))
    } catch {
      case _: Throwable => None
    }
  }

  def getPrimaryName(address: String): String = {
    val reverseRecords = new Contract(CaverFactory.caver,
                                      Constants.KNSReverseRecordsABI,
                                      ReverseRecords)
    reverseRecords
      .call("getName", new Address(address))
      .get(0)
      .getValue
      .toString
  }
}
