package io.klaytn.contract.lib

import com.klaytn.caver.Caver

object KIP7MetadataReaderTestMain {
  def main(args: Array[String]): Unit = {
    read()
  }

  private def read(): Unit = {
    val contractAddress = "0xe815a060b9279eba642f8c889fab7afc0d0aca63"

    val caver = new Caver("http://en2.klayoff.com:8551")
    val reader = new KIP7MetadataReader(caver)

    println(reader.read(contractAddress))
    println(
      reader.balanceOf(contractAddress,
                       "0xf4393987612f8df3e1c1807570f80a4f228367fe"))
  }
}
