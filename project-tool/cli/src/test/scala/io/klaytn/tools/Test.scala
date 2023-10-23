package io.klaytn.tools

import org.scalatest.funsuite.AnyFunSuite
import com.klaytn.caver.Caver
import io.klaytn.contract.lib.KIP17MetadataReader

object Test extends AnyFunSuite {
  test("TokenTest") {
    val caver = new Caver("https://api.cypress.klaytn.net:8651")
    val reader = new KIP17MetadataReader(caver)
    val result = reader
      .tokenURI("0x75acaaefe4c01690456589371fd70c67e12ac21b", BigInt(79))
      .getOrElse("-")
    println("token URI result: " + result)
    throw new RuntimeException(s"token URI result: $result")
  }
}
