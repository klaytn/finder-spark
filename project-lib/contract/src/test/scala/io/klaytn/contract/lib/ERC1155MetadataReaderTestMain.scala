package io.klaytn.contract.lib

import com.klaytn.caver.Caver

object ERC1155MetadataReaderTestMain {
  def main(args: Array[String]): Unit = {
    read()
  }

  private def read(): Unit = {
    val contractAddress = "0xc59b50d1ab5d644ab09a6d9b74b1445b0475e5a6"

    val caver = new Caver("http://en2.klayoff.com:8551")
    val reader = new ERC1155MetadataReader(caver)

    println(reader.read(contractAddress))
    println(reader.uri(contractAddress, BigInt(2)))
    println(reader.totalSupply(contractAddress, BigInt(2)))
    println(
      reader.balanceOf(contractAddress,
                       "0x2cc2448ccf89183045eaf0e06eaed3c5ebd07da1",
                       BigInt(2)))
  }
}
