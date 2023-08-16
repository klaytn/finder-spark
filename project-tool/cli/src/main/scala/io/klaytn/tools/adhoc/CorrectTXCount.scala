package io.klaytn.tools.adhoc

import com.klaytn.caver.Caver
import io.klaytn.tools.dsl.db

import java.io.{BufferedWriter, File, FileWriter}
import scala.collection.mutable

object CorrectTXCount {
  def main(args: Array[String]): Unit = {
    new CorrectTXCount().run(args)
  }
}

class CorrectTXCount {
  def path(typ: String): String = s"/home/ssm-user/tx_count.$typ"

  def getCaverUrlAndDBName(typ: String): (String, String) = {
    typ match {
      case "cypress" => ("http://10.2.42.223:8551", "finder0101")
      case _         => ("http://10.2.41.192:8551", "baobab")
    }
  }

  def updateTXCount(dbName: String,
                    chainCount: BigInt,
                    address: String): Unit = {
    db.withDB(dbName) { c =>
      val pstmt =
        c.prepareStatement(
          s"UPDATE `accounts` SET `total_transaction_count`=? WHERE `address`=?")

      pstmt.setLong(1, chainCount.toLong)
      pstmt.setString(2, address)

      pstmt.executeUpdate()
      pstmt.close()
    }
  }

  def step1(typ: String): Unit = {
    val (caverUrl, dbName) = getCaverUrlAndDBName(typ)
    val caver = new Caver(caverUrl)

    val maxId = db.withDB(dbName) { c =>
      val pstmt = c.prepareStatement("SELECT MAX(`id`) FROM `accounts`")
      val rs = pstmt.executeQuery()
      val res =
        if (rs.next()) rs.getLong(1)
        else 0L
      rs.close()
      pstmt.close()

      res
    }

    val file = new File(path(typ))
    val bw = new BufferedWriter(new FileWriter(file))

    val getCount = 100000
    var idx = 0
    0 to (maxId / getCount).toInt foreach { idIdx =>
      val holders = mutable.ArrayBuffer.empty[(String, Long)]
      db.withDB(dbName) { c =>
        val pstmt =
          c.prepareStatement(
            "SELECT `address`, `total_transaction_count` FROM `accounts` WHERE id > ? AND id <= ?")
        pstmt.setLong(1, idIdx * getCount)
        pstmt.setLong(2, idIdx * getCount + getCount)
        val rs = pstmt.executeQuery()
        while (rs.next()) {
          holders.append((rs.getString(1), rs.getLong(2)))
        }
        rs.close()
        pstmt.close()
      }

      println("finish load data...")

      val totalSize = holders.size
      holders.foreach {
        case (address, dbCount) =>
          idx += 1
          if (idx % 10 == 0) println(s"$idx / $totalSize")
          try {
            val chainCount = BigInt(
              caver.rpc.klay
                .getTransactionCount(address)
                .send()
                .getValue).toLong
            if (dbCount != chainCount) {
              updateTXCount(dbName, chainCount, address)
              bw.write(s"$address\t$chainCount\t$dbCount\n")
            }
          } catch {
            case _: Throwable =>
              bw.write(s"ERROR\t$address\t$dbCount\n")
          }
      }
    }
    bw.close()
  }

  def run(args: Array[String]): Unit = {
    val typ = args.head
    step1(typ)
  }
}
