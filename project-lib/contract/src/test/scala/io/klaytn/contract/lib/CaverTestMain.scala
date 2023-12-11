package io.klaytn.contract.lib

import com.klaytn.caver.Caver
import org.scalatest.funsuite.AnyFunSuite

class CaverTestMain extends AnyFunSuite {

  test("balanceOf") {
    balanceOf()
  }

  private def balanceOf(): Unit = {
    val address = "0x000000000000000000000000000000000000dead"
    val blockNumber = 139698262L
    val caver = new Caver("https://public-en.klaytnfinder.io/v1/cypress")
    val res = BigInt(caver.rpc.klay.getBalance(address).send().getValue)
    println(s"balanceOf: ${res}")
  }
}

// contract-lib
// sudo ./sbt contract-lib/clean contract-lib/assembly
// sudo ./sbt testOnly io.klaytn.contract.lib.CaverTestMain
