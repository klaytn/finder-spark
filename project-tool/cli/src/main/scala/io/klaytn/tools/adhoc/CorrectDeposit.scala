package io.klaytn.tools.adhoc

import com.klaytn.caver.Caver
import io.klaytn.tools.dsl.db

import java.io.{BufferedWriter, File, FileWriter}
import scala.io.Source

object CorrectDeposit {
  def main(args: Array[String]): Unit = {
    new CorrectDeposit().run(args)
  }
}

class CorrectDeposit {
  def path(typ: String): String = s"/home/ssm-user/deposit.$typ"

  def getCaverUrlAndDBName(typ: String): (String, String) = {
    typ match {
      case "cypress" => ("http://10.2.42.223:8551", "finder03")
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

  def hexToBigInt(s: String): BigInt = {
    if (s.isEmpty) {
      BigInt(0)
    } else if (s.startsWith("0x")) {
      val normalize0 = normalize(s, false)
      if (normalize0.isEmpty) {
        BigInt(0)
      } else {
        BigInt(normalize0, 16)
      }
    } else {
      BigInt(s, 16)
    }
  }

  def normalize(s: String, withPrefix0x: Boolean): String = {
    val result0 = s.replaceFirst("0x", "").replaceFirst("[0]*", "")
    val result = if (result0.isEmpty) "0" else result0
    if (withPrefix0x) s"0x$result"
    else result
  }

  def to64BitsHex(bigInt: BigInt): String = {
    f"0x${bigInt.toString(16)}%64s".replace(' ', '0')
  }

  def step1(typ: String): Unit = {
    val (caverUrl, dbName) = getCaverUrlAndDBName(typ)
    val caver = new Caver(caverUrl)

    val file = new File(path(typ))
    val bw = new BufferedWriter(new FileWriter(file))

    val src = Source.fromFile("/home/ssm-user/cypress.deposit")

    var idx = 0

    src.getLines().foreach { line =>
      val s = line.split("\t")
      val address = s(0)
      val holder = s(1)
      val blockNumber = s(2).toLong

      idx += 1
      if (idx % 100 == 0) println(idx + "/" + 115418)

      try {
        val kip7 = caver.kct.kip7.create(address)
        val chainBalance = BigInt(kip7.balanceOf(holder))

        val dbBalance = db.withDB(db.getDatasource("finder03r")) { c =>
          val pstmt =
            c.prepareStatement(
              "SELECT amount FROM token_holders WHERE contract_address=? AND holder_address=?")
          pstmt.setString(1, address)
          pstmt.setString(2, holder)
          val rs = pstmt.executeQuery()
          val dbB =
            if (rs.next()) hexToBigInt(rs.getString(1))
            else BigInt(0)
          pstmt.close()
          dbB
        }

        if (dbBalance != chainBalance) {
          bw.write(s"$address\t$holder\t$dbBalance\t$chainBalance\n")
          if (dbBalance == 0) {
            val ts = db.withDB(db.getDatasource("finder0101r")) { c =>
              val pstmt = c.prepareStatement(
                "select timestamp from blocks where number=?")
              pstmt.setLong(1, blockNumber)
              val rs = pstmt.executeQuery()
              val ts =
                if (rs.next()) rs.getInt(1)
                else 0
              pstmt.close()
              ts
            }

            db.withDB(db.getDatasource("finder03")) { c =>
              val pstmt = c.prepareStatement(
                "INSERT INTO token_holders (contract_address, holder_address, amount, last_transaction_time) values (?,?,?,?)")
              pstmt.setString(1, address)
              pstmt.setString(2, holder)
              pstmt.setString(3, to64BitsHex(chainBalance))
              pstmt.setInt(4, ts)
              pstmt.execute()
              pstmt.close()
            }
          } else {
            db.withDB(db.getDatasource("finder03")) { c =>
              val pstmt = c.prepareStatement(
                "update token_holders set amount=? where contract_address = ? and holder_address = ?")
              pstmt.setString(1, to64BitsHex(chainBalance))
              pstmt.setString(2, address)
              pstmt.setString(3, holder)
              pstmt.execute()
              pstmt.close()
            }
          }
        }
      } catch {
        case _: Throwable =>
      }
    }
    bw.close()
  }

  def run(args: Array[String]): Unit = {
    val typ = args.head
    step1(typ)
  }
}
