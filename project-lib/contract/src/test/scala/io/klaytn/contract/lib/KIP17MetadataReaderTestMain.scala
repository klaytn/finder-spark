package io.klaytn.contract.lib

import com.klaytn.caver.Caver

object KIP17MetadataReaderTestMain {
  def main(args: Array[String]): Unit = {
    read()
  }

  private def read(): Unit = {
    val contractAddress = "0x19c8f636118dfb7b6cbe2620a7653e229f8b8011"

    val caver = new Caver("http://en2.klayoff.com:8551")
    val reader = new KIP17MetadataReader(caver)

    println(reader.read(contractAddress))
    println(
      reader.balanceOf(contractAddress,
                       "0xb7a8251b773df1bf6641f6d0fd31e2b8b3b5df5b"))
    println(reader.ownerOf(contractAddress, BigInt(755)))
  }
}
