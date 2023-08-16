package io.klaytn.apps.restore

import io.klaytn.service.CaverService
import org.json4s.NoTypeHints
import org.json4s.native.Serialization
import org.json4s.native.Serialization.write

import java.io.{BufferedWriter, File, FileWriter}

// Run: java -cp klaytn-spark.jar io.klaytn.apps.restore.BlockAndInternalTXRestoreBatch
object BlockAndInternalTXRestoreBatch extends App {
  private val caverService = CaverService.of()

  def writeFile(filename: String, s: String): Unit = {
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(s)
    bw.close()
  }

  implicit val formats = Serialization.formats(NoTypeHints)

  Seq(80784919)
    .foreach { blockNumber =>
      val blockFile = s"/home/ssm-user/jar/block-$blockNumber.json"
      if (!new java.io.File(blockFile).exists()) {
        println(s"block: $blockNumber")
        val block = caverService.getBlock(blockNumber)
        val blockJson = write(block)

        writeFile(blockFile, blockJson)
        println(s"complete block: $blockNumber")

        //        val itxFile = s"/home/ssm-user/jar/itx-$blockNumber.json"
        //        println(s"itx: $blockNumber")
        //        val internalTransaction = InternalTransaction(
        //          blockNumber,
        //          block.result.transactions.map { tx =>
        //            val start = System.currentTimeMillis()
        //            val result = CaverUtil.getInternalTransaction(tx.transactionHash) match {
        //              case Some(i) => i
        //              case _ =>
        //                println(s"time: ${System.currentTimeMillis() - start} ms ; tx hash: ${tx.transactionHash}")
        //                InternalTransactionContent("0", Some("0x"), None, None, None, None, None, None, None, None, None, None)
        //            }
        //            result
        //          }
        //        )
        //
        //        println(s"complete itx: $blockNumber")
        //        val internalTransactionJson = write(internalTransaction)
        //        writeFile(itxFile, internalTransactionJson)
      } else {
        println(s"exist file: $blockNumber")
      }
    }
}
