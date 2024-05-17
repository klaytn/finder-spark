package io.klaytn.service

import com.klaytn.caver.Caver
import com.typesafe.config.{Config, ConfigFactory}
import io.klaytn._
import io.klaytn.contract.lib.ContractType.{
  ERC1155,
  ERC20,
  ERC721,
  KIP17,
  KIP37,
  KIP7
}
import io.klaytn.contract.lib._
import io.klaytn.model.finder.{Contract, ContractType}
import io.klaytn.utils.LockUtil

import scala.util.{Random, Try}

object CaverContractService {
  def of(): CaverContractService = of(ConfigFactory.load())

  def of(config: Config): CaverContractService = {
    of(config.getString("spark.app.caver.url"))
  }

  def of(caverUrl: String): CaverContractService = {
    val caver = new Caver(caverUrl)
    of(caver)
  }

  def of(caver: Caver): CaverContractService = {
    new CaverContractService(caver)
  }
}

class CaverContractService(caver: LazyEval[Caver]) extends Serializable {
  def getType(contractAddress: String,
              byteCodes: String): io.klaytn.model.finder.ContractType.Value = {
    val checker = new ContractChecker(caver)
    val contentType = checker.check(contractAddress, byteCodes)
    if (contentType == null) {
      return io.klaytn.model.finder.ContractType.Custom
    }
    contentType match {
      case KIP7    => io.klaytn.model.finder.ContractType.KIP7
      case KIP17   => io.klaytn.model.finder.ContractType.KIP17
      case KIP37   => io.klaytn.model.finder.ContractType.KIP37
      case ERC20   => io.klaytn.model.finder.ContractType.ERC20
      case ERC721  => io.klaytn.model.finder.ContractType.ERC721
      case ERC1155 => io.klaytn.model.finder.ContractType.ERC1155
      case _       => io.klaytn.model.finder.ContractType.Custom
    }
  }

  def getKIP17URI(contractAddress: String, tokenId: BigInt): String = {
    val reader = new KIP17MetadataReader(caver)
    reader.tokenURI(contractAddress, tokenId.bigInteger).getOrElse("-")
  }
  def getERC721URI(contractAddress: String, tokenId: BigInt): String = {
    val reader = new ERC721MetadataReader(caver)
    reader.tokenURI(contractAddress, tokenId.bigInteger).getOrElse("-")
  }
  def getKIP37URI(contractAddress: String, id: BigInt): String = {
    val reader = new KIP37MetadataReader(caver)
    reader.uri(contractAddress, id.bigInteger).getOrElse("-")
  }
  def getERC1155URI(contractAddress: String, id: BigInt): String = {
    val reader = new ERC1155MetadataReader(caver)
    reader.uri(contractAddress, id.bigInteger).getOrElse("-")
  }

  def getERC20(contractAddress: String): Contract = {
    val reader = new ERC20MetadataReader(caver)
    val info = reader.read(contractAddress)
    Contract(info.address,
             io.klaytn.model.finder.ContractType.ERC20,
             info.name,
             info.symbol,
             info.decimals,
             info.totalSupply)
  }

  def getERC721(contractAddress: String): Contract = {
    val reader = new ERC721MetadataReader(caver)
    val info = reader.read(contractAddress)
    Contract(info.address,
             io.klaytn.model.finder.ContractType.ERC721,
             info.name,
             info.symbol,
             None,
             info.totalSupply)
  }

  def getERC1155(contractAddress: String): Contract = {
    val reader = new ERC1155MetadataReader(caver)
    val info = reader.read(contractAddress)
    Contract(info.address,
             io.klaytn.model.finder.ContractType.ERC1155,
             None,
             None,
             None,
             None)
  }

  def getKIP7(contractAddress: String): Contract = {
    val reader = new KIP7MetadataReader(caver)
    val info = reader.read(contractAddress)
    Contract(info.address,
             io.klaytn.model.finder.ContractType.KIP7,
             info.name,
             info.symbol,
             info.decimals,
             info.totalSupply)
  }

  def getKIP17(contractAddress: String): Contract = {
    val reader = new KIP17MetadataReader(caver)
    val info = reader.read(contractAddress)
    Contract(info.address,
             io.klaytn.model.finder.ContractType.KIP17,
             info.name,
             info.symbol,
             None,
             info.totalSupply)
  }

  def getKIP37(contractAddress: String): Contract = {
    val reader = new KIP37MetadataReader(caver)
    val info = reader.read(contractAddress)
    Contract(info.address,
             io.klaytn.model.finder.ContractType.KIP37,
             None,
             None,
             None,
             None)
  }

  def getKIP7MetadataReader: KIP7MetadataReader = new KIP7MetadataReader(caver)
  def getKIP17MetadataReader: KIP17MetadataReader =
    new KIP17MetadataReader(caver)
  def getKIP37MetadataReader: KIP37MetadataReader =
    new KIP37MetadataReader(caver)
  def getERC20MetadataReader: ERC20MetadataReader =
    new ERC20MetadataReader(caver)
  def getERC721MetadataReader: ERC721MetadataReader =
    new ERC721MetadataReader(caver)
  def getERC1155MetadataReader: ERC1155MetadataReader =
    new ERC1155MetadataReader(caver)

  def getTotalSupplyAndDecimal(
      contractAddress: String,
      contractType: ContractType.Value): (Option[BigInt], Option[Int]) = {
    contractType match {
      case io.klaytn.model.finder.ContractType.KIP7 =>
        val kip7 = new KIP7MetadataReader(caver)
        (kip7.totalSupply(contractAddress), kip7.decimals(contractAddress))
      case io.klaytn.model.finder.ContractType.ERC20 =>
        val erc20 = new ERC20MetadataReader(caver)
        (erc20.totalSupply(contractAddress), erc20.decimals(contractAddress))
      case io.klaytn.model.finder.ContractType.KIP17 =>
        val kip17 = new KIP17MetadataReader(caver)
        (kip17.totalSupply(contractAddress), None)
      case io.klaytn.model.finder.ContractType.ERC721 =>
        val erc721 = new ERC721MetadataReader(caver)
        (erc721.totalSupply(contractAddress), None)
      case _ =>
        (None, None)
    }
  }

  def getToken(contractAddress: String): Contract =
    Try(getKIP7(contractAddress)).getOrElse(getERC20(contractAddress))

  def getNFT(contractAddress: String): Contract =
    Try(getKIP17(contractAddress)).getOrElse(getERC721(contractAddress))

  def getMultiToken(contractAddress: String): Contract =
    Try(getKIP37(contractAddress)).getOrElse(getERC1155(contractAddress))

  def getContractWithLock(contractAddress: String,
                          byteCodes: String): Contract = {
    LockUtil.lock(s"getContract:${Random.nextInt(20)}")(() =>
      getContract(contractAddress, byteCodes))
  }

  def getContract(contractAddress: String, byteCodes: String): Contract = {
    val checker = new ContractChecker(caver)

    try {
      val check = checker.check(contractAddress, byteCodes)
      if (check.isDefined) {
        check.get match {
          case KIP7    => getToken(contractAddress)
          case KIP17   => getNFT(contractAddress)
          case KIP37   => getMultiToken(contractAddress)
          case ERC20   => getERC20(contractAddress)
          case ERC721  => getERC721(contractAddress)
          case ERC1155 => getERC1155(contractAddress)
          case _       => Contract.empty(contractAddress)
        }
      } else {
        Contract.empty(contractAddress)
      }
    } catch {
      case e: Throwable =>
        e.printStackTrace()
        Contract.empty(contractAddress)
    }
  }
}
