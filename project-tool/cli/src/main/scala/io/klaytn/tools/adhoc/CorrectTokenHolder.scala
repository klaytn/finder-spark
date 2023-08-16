package io.klaytn.tools.adhoc

import com.klaytn.caver.Caver
import io.klaytn.contract.lib.KIP7MetadataReader
import io.klaytn.tools.dsl.db

import java.io.{BufferedWriter, File, FileWriter}
import scala.collection.mutable
import scala.io.Source

object CorrectTokenHolder {
  def main(args: Array[String]): Unit = {
    new CorrectTokenHolder().run(args)
  }
}

class CorrectTokenHolder {
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

  def path(typ: String): String = s"/home/ssm-user/holder.$typ"

  def getCaverUrlAndDBName(typ: String): (String, String) = {
    typ match {
      case "cypress" => ("http://10.2.42.223:8551", "finder03")
      case _         => ("http://10.2.41.192:8551", "baobab")
    }
  }

  def updateTokenHolders(dbName: String,
                         chainAmount: String,
                         contract: String,
                         holder: String): Unit = {
    db.withDB(dbName) { c =>
      val pstmt =
        c.prepareStatement(
          s"UPDATE `token_holders` SET `amount`=? WHERE `contract_address`=? AND `holder_address`=?")

      pstmt.setString(1, chainAmount)
      pstmt.setString(2, contract)
      pstmt.setString(3, holder)

      pstmt.executeUpdate()
      pstmt.close()
    }
  }

  def step1(typ: String): Unit = {
    val (caverUrl, dbName) = getCaverUrlAndDBName(typ)
    val caver = new Caver(caverUrl)

    val holders = mutable.ArrayBuffer.empty[(String, String, BigInt)]
    db.withDB(dbName) { c =>
      val pstmt = c.prepareStatement(
        "SELECT `contract_address`, `holder_address`, `amount` FROM `token_holders`")
      val rs = pstmt.executeQuery()
      while (rs.next()) {
        holders.append(
          (rs.getString(1), rs.getString(2), hexToBigInt(rs.getString(3))))
      }
      rs.close()
      pstmt.close()
    }

    println("finish load data...")

    val file = new File(path(typ))
    val bw = new BufferedWriter(new FileWriter(file))

    val kip7 = new KIP7MetadataReader(caver)
    var idx = 0
    val totalSize = holders.size
    holders.foreach {
      case (contractAddress, holderAddress, amount) =>
        idx += 1
        if (idx % 10 == 0) println(s"$idx / $totalSize")
        try {
          val chainAmount =
            kip7.balanceOf(contractAddress, holderAddress).getOrElse(BigInt(0))
          if (amount != chainAmount) {
            updateTokenHolders(dbName,
                               to64BitsHex(chainAmount),
                               contractAddress,
                               holderAddress)
            bw.write(
              s"$contractAddress\t$holderAddress\t$amount\t$chainAmount\n")
          }
        } catch {
          case _: Throwable =>
            println(s"ERROR: $contractAddress\t$holderAddress")
        }
    }

    bw.close()
  }

  def step2(typ: String): Unit = {
    val src = Source.fromFile(path(typ))

    val (caverUrl, dbName) = getCaverUrlAndDBName(typ)
    val caver = new Caver(caverUrl)
    val kip7 = new KIP7MetadataReader(caver)

    src.getLines().zipWithIndex.foreach {
      case (line, idx) =>
        val s = line.split("\t")
        val (contractAddress, holderAddress) = (s(0), s(1))

        val chainAmount = to64BitsHex(
          kip7.balanceOf(contractAddress, holderAddress).getOrElse(BigInt(0)))

        updateTokenHolders(dbName, chainAmount, contractAddress, holderAddress)

        val dbBalance = db.withDB(dbName) { c =>
          val pstmt =
            c.prepareStatement(
              "SELECT `amount` FROM `token_holders` WHERE `contract_address`=? AND `holder_address`=?")

          pstmt.setString(1, contractAddress)
          pstmt.setString(2, holderAddress)

          val rs = pstmt.executeQuery()
          val result =
            if (rs.next()) hexToBigInt(rs.getString(1)) else BigInt(0)

          pstmt.close()
          result
        }
        val chainAmount2 = to64BitsHex(
          kip7.balanceOf(contractAddress, holderAddress).getOrElse(BigInt(0)))

        val c1 = hexToBigInt(chainAmount)
        val c2 = hexToBigInt(chainAmount2)

        println(
          s"$idx] $contractAddress\t$holderAddress\t$c1\t$dbBalance\t$c2\t${c1 == dbBalance}\t${c2 == dbBalance}")
    }
    src.close()
  }

  def run(args: Array[String]): Unit = {
    val typ = args.head

    if (args.last == "step1") step1(typ)
    else if (args.last == "step2") step2(typ)
    else println("usage: CorrectTokenHolder typ step#")
  }
}
