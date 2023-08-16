package io.klaytn.contract.lib

import com.klaytn.caver.Caver

object ERC20MetadataReaderTestMain {
  def main(args: Array[String]): Unit = {
    read()
  }

  private def read(): Unit = {
    val contractAddress =
      Seq("0x53afce8bd5b784ed1d5960f1c4f804ab4ab7476e")

    val caver = new Caver("https://api.baobab.klaytn.net:8651")
    val reader = new KIP7MetadataReader(caver)

    contractAddress.foreach { x =>
      println(x)
      println(reader.read(x))
      println(reader.balanceOf(x, "0x327ac2db919d37be8bf266119fb74a22539bf5bf"))
    // KIP7(address=0xcf87f94fd8f6b6f0b479771f10df672f99eada63, symbol=CLA, name=ClaimSwap, decimals=18, totalSupply=186624000000000000000000000)
    // 541875153032990166566
    // KIP7(address=0x45dbbbcdff605af5fe27fd5e93b9f3f1bb25d429, symbol=MUDOL, name=Mudol Token, decimals=18, totalSupply=360000000000000000000000000)
    // 0
    }
  }
}
