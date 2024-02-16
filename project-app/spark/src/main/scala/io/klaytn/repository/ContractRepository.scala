package io.klaytn.repository

import io.klaytn.client.FinderRedis
import io.klaytn.dsl.db.withDB
import io.klaytn.model.finder.{Contract, ContractType}
import io.klaytn.persistent.ContractDTO
import io.klaytn.utils.{SlackUtil, Utils}
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.exception.ExceptionUtils

object ContractRepository {
  val ContractDB: String = "finder0101"
  val ContractTable = "contracts"
}

import scala.collection.mutable

abstract class ContractRepository extends AbstractRepository {
  import ContractRepository._

  private def deleteCache(contractAddress: String): Unit = {
    FinderRedis.del(s"cache/contract-by-address::$contractAddress")
  }

  def insert(contract: Contract,
             transferCount: Int,
             createdTimestamp: Long,
             txError: Int): Option[Contract] = {
    withDB(ContractDB) { c =>
      val pstmt = c.prepareStatement(
        s"INSERT INTO $ContractTable (`contract_address`,`contract_type`,`name`,`symbol`,`decimal`,`total_supply`,`tx_error`,`total_supply_order`,`total_transfer`,`created_at`)" +
          " VALUES (?,?,?,?,?,?,?,?,?, IFNULL(?, NOW()) ) ON DUPLICATE KEY UPDATE `contract_type`=?,`name`=?,`symbol`=?,`decimal`=?,`total_supply`=?,`total_supply_order`=?,`total_transfer`=`total_transfer`+?,`created_at`=IFNULL(?, NOW()),`tx_error`=?")

      val name = contract.name.getOrElse("")
      val symbol = contract.symbol.getOrElse("")
      val decimal = contract.decimal.getOrElse(0)
      val bTotalSupply = contract.totalSupply.getOrElse(BigInt(0))
      val totalSupply = bTotalSupply.toString()
      val totalSupplyOrder = Utils.getTotalSupplyOrder(decimal, bTotalSupply)

      pstmt.setString(1, contract.contractAddress)
      pstmt.setInt(2, contract.contractType.id)
      pstmt.setString(3, name)
      pstmt.setString(4, symbol)
      pstmt.setInt(5, decimal)
      pstmt.setString(6, totalSupply)
      pstmt.setInt(7, txError)
      pstmt.setString(8, totalSupplyOrder)
      pstmt.setInt(9, transferCount)
      pstmt.setString(10, sdf.format(createdTimestamp))

      pstmt.setInt(11, contract.contractType.id)
      pstmt.setString(12, name)
      pstmt.setString(13, symbol)
      pstmt.setInt(14, decimal)
      pstmt.setString(15, totalSupply)
      pstmt.setString(16, totalSupplyOrder)
      pstmt.setInt(17, transferCount)
      pstmt.setString(18, sdf.format(createdTimestamp))
      pstmt.setInt(19, txError)

      try {
        pstmt.execute()
      } catch {
        case e: Throwable =>
          SlackUtil.sendMessage(s"""
                                   |error contract register
                                   |address: ${contract.contractAddress}
                                   |type: ${contract.contractType.id}
                                   |name: $name
                                   |symbol: $symbol
                                   |decimal: $decimal
                                   |totalSupply: $totalSupply
                                   |totalSupplyOrder: $totalSupplyOrder
                                   |timestamp: ${sdf.format(createdTimestamp)}
                                   |createdTimestamp: $createdTimestamp
                                   |${e.getMessage}
                                   |${StringUtils.abbreviate(
                                     ExceptionUtils.getStackTrace(e),
                                     500)}
                                   |""".stripMargin)
      } finally {
        pstmt.close()
      }
    }

    deleteCache(contract.contractAddress)
    findContract(contract.contractAddress)
  }

  def updateTotalTransfer(
      data: Seq[(String, Int)]): Seq[((String, Int), Int)] = {
    val results = data.map {
      case (contractAddress, totalTransfer) =>
        withDB(ContractDB) { c =>
          val pstmt =
            c.prepareStatement(
              s"UPDATE $ContractTable SET `total_transfer`=`total_transfer`+? WHERE `contract_address`=?")

          pstmt.setBigDecimal(1, BigDecimal(totalTransfer).bigDecimal)
          pstmt.setString(2, contractAddress)
          val updateResult = pstmt.executeUpdate()
          pstmt.close()
          updateResult
        }
    }

    data.zipWithIndex.map {
      case (d, index) =>
        deleteCache(d._1)
        (d, results(index))
    }
  }

  def updateTotalSupply(contractAddress: String,
                        totalSupply: BigInt,
                        decimal: Int,
                        totalSupplyOrder: String): Unit = {
    withDB(ContractDB) { c =>
      val pstmt = c.prepareStatement(
        s"UPDATE $ContractTable SET `total_supply`=?,`total_supply_order`=?,`decimal`=? WHERE `contract_address`=?")

      pstmt.setString(1, totalSupply.toString())
      pstmt.setString(2, totalSupplyOrder)
      pstmt.setInt(3, decimal)
      pstmt.setString(4, contractAddress)

      pstmt.executeUpdate()

      pstmt.close()
    }

    deleteCache(contractAddress)
  }

  def updateHolderCount(data: Seq[(String, Long)], replace: Boolean): Unit = {
    if (data.nonEmpty) {
      withDB(ContractDB) { c =>
        val pstmt =
          if (replace)
            c.prepareStatement(
              s"UPDATE $ContractTable SET `holder_count`=? WHERE `contract_address`=?")
          else
            c.prepareStatement(
              s"UPDATE $ContractTable SET `holder_count`=`holder_count`+? WHERE `contract_address`=?")

        data.foreach {
          case (contractAddress, holderCount) =>
            pstmt.setLong(1, holderCount)
            pstmt.setString(2, contractAddress)
            pstmt.addBatch()
            pstmt.clearParameters()
        }
        execute(pstmt)
        pstmt.close()
      }
      data.foreach(x => deleteCache(x._1))
    }
  }

  def updateContract(contract: Contract): Unit = {
    val decimal = contract.decimal.getOrElse(0)
    val totalSupply = contract.totalSupply.getOrElse(BigInt(0))
    val totalSupplyOrder = Utils.getTotalSupplyOrder(decimal, totalSupply)

    withDB(ContractDB) { c =>
      val pstmt = c.prepareStatement(
        s"UPDATE $ContractTable SET `contract_type`=?,`name`=?,`symbol`=?,`decimal`=?,`total_supply`=?,`total_supply_order`=? WHERE `contract_address`=?")
      pstmt.setInt(1, contract.contractType.id)
      pstmt.setString(2, contract.name.orNull)
      pstmt.setString(3, contract.symbol.orNull)
      pstmt.setInt(4, decimal)
      pstmt.setString(5, totalSupply.toString)
      pstmt.setString(6, totalSupplyOrder)
      pstmt.setString(7, contract.contractAddress)
      pstmt.executeUpdate()
      pstmt.close()
    }

    deleteCache(contract.contractAddress)
  }

//  def updateTotalTransferByEventLog(eventLogs: Seq[RefinedEventLog]): Unit = {
//    withDB(datasource(ContractDB)) { c =>
//      val pstmt =
//        c.prepareStatement(s"UPDATE $ContractTable SET `total_transfer`=`total_transfer`+? WHERE `contract_address`=?")
//
//      eventLogs
//        .map { eventLog =>
//          val (address, _, _, _, _) = eventLog.extractTransferInfo()
//          (address, 1)
//        }
//        .groupBy(_._1)
//        .mapValues(_.map(_._2).sum)
//        .foreach {
//          case (contractAddress, transferCount) =>
//            pstmt.setLong(1, transferCount)
//            pstmt.setString(2, contractAddress)
//            try {
//              // ignore exception... The count is periodically calibrated to...
//              pstmt.executeUpdate()
//            } catch {
//              case _: Throwable =>
//            }
//        }
//
//      pstmt.close()
//    }
//  }

  def findAllContractByAddress(addressList: Seq[String]): Seq[ContractDTO] = {
    val placeHolder =
      Range.inclusive(0, addressList.size - 1).map(_ => "?").mkString(",")
    withDB(ContractDB) { c =>
      val pstmt = c.prepareStatement(
        s"SELECT * FROM $ContractTable WHERE contract_address IN ($placeHolder)")
      addressList.zipWithIndex.foreach {
        case (address, idx) =>
          pstmt.setString(idx + 1, address)
      }

      val rs = pstmt.executeQuery()

      val list = new mutable.ListBuffer[ContractDTO]()
      while (rs.next()) {
        val contract = ContractDTO(
          contractAddress = rs.getString("contract_address"),
          contractType = ContractType.from(rs.getInt("contract_type")),
          name = Some(rs.getString("name")),
          symbol = Some(rs.getString("symbol")),
          decimal = Some(rs.getInt("decimal")),
          totalSupply = Some(BigInt(rs.getString("total_supply"))),
          verified = rs.getInt("verified") == 1
        )
        list.append(contract)
      }
      rs.close()
      pstmt.close()
      list
    }
  }

  def findContract(address: String): Option[Contract] = {
    withDB(ContractDB) { c =>
      val pstmt = c.prepareStatement(
        s"SELECT * FROM $ContractTable WHERE contract_address=?")
      pstmt.setString(1, address)

      val rs = pstmt.executeQuery()

      val contract = if (rs.next()) {
        Contract(
          contractAddress = address,
          contractType = ContractType.from(rs.getInt("contract_type")),
          name = Some(rs.getString("name")),
          symbol = Some(rs.getString("symbol")),
          decimal = Some(rs.getInt("decimal")),
          totalSupply = Some(BigInt(rs.getString("total_supply")))
        )
      } else {
        null
      }
      rs.close()
      pstmt.close()

      Option(contract)
    }
  }

  def selectBurnInfo(contractAddress: String): (String, Int) = {
    withDB(ContractDB) { c =>
      val pstmt = c.prepareStatement(
        s"SELECT `burn_amount`,`total_burn` FROM $ContractTable WHERE contract_address=?")
      pstmt.setString(1, contractAddress)
      val rs = pstmt.executeQuery()
      val result = if (rs.next()) {
        val amount = if (rs.getString(1) == null) "0x0" else rs.getString(1)
        (amount, rs.getInt(2))
      } else {
        ("0x0", 0)
      }
      pstmt.close()

      result
    }
  }

  def updateBurnInfo(contractAddress: String,
                     amount: String,
                     count: Int): Unit = {
    withDB(ContractDB) { c =>
      val pstmt =
        c.prepareStatement(
          s"UPDATE $ContractTable SET `burn_amount`=?,`total_burn`=? WHERE contract_address=?")
      pstmt.setString(1, amount)
      pstmt.setInt(2, count)
      pstmt.setString(3, contractAddress)
      pstmt.execute()
      pstmt.close()
    }
    deleteCache(contractAddress)
  }

  def updateImplementationAddress(contractAddress: String,
                                  implementationAddress: String): Unit = {
    withDB(ContractDB) { c =>
      val pstmt =
        c.prepareStatement(
          s"UPDATE $ContractTable SET `implementation_address`=? WHERE contract_address=?")
      pstmt.setString(1, implementationAddress)
      pstmt.setString(2, contractAddress)
      pstmt.execute()
      pstmt.close()
    }
    deleteCache(contractAddress)

    withDB(ContractDB) { c =>
      val pstmt =
        c.prepareStatement(
          s"UPDATE $ContractTable SET `contract_type`=127 WHERE contract_address=? AND `contract_type`!=127")
      pstmt.setString(1, implementationAddress)
      pstmt.execute()
      pstmt.close()
    }
    deleteCache(implementationAddress)
  }
}
