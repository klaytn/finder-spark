package io.klaytn.contract.lib

import com.klaytn.caver.Caver

object KIP37MetadataReaderTestMain {
  def main(args: Array[String]): Unit = {
    read()
  }

  private def read(): Unit = {
    val contractAddress = "0xfe1970e7fba02c2ab7721840eca0277d5ee6b482"

    val caver = new Caver("http://en2.klayoff.com:8551")
    val reader = new KIP37MetadataReader(caver)

    println(reader.read(contractAddress))
    println(reader.uri(contractAddress, BigInt(2)))
    println(reader.totalSupply(contractAddress, BigInt(2)))
    println(
      reader.balanceOf(contractAddress,
                       "0xf46e9b6902fe681bcde127464e4623051d8e7f15",
                       BigInt(2)))
  }
}
