package io.klaytn.apps.restore.tokenURI

import io.klaytn.utils.SlackUtil
import io.klaytn.utils.spark.{KafkaStreamingHelper}

object TokenURI extends KafkaStreamingHelper {
  import TokenURIDeps._

  def tokenURI(id: Long, limit: Long): (Int, Int) = {
    val totalTokenURIs =
      scala.collection.mutable.ListBuffer.empty[(String, String, String)]
    val inventories =
      holderPersistentAPI.getInventoriesByIdRange(id, id + limit)
    val inventorySize = inventories.size
    val inventoriesRDD = sc.parallelize(inventories, 50)
    inventoriesRDD
      .mapPartitions { partition =>
        val tokenURIs =
          scala.collection.mutable.ListBuffer.empty[(String, String, String)]
        partition.foreach { x =>
          val contractAddress = x._1.contractAddress
          val tokenId = x._1.tokenId
          val tokenURI =
            contractService.getFreshTokenURI(
              x._2,
              contractAddress,
              tokenId
            )
          if (tokenURI != "-") {
            val t = (contractAddress, tokenId.toString, tokenURI)
            tokenURIs += t
          }
        }
        tokenURIs.iterator
      }
      .collect()
      .foreach { x: (String, String, String) =>
        totalTokenURIs += x
      }
    totalTokenURIs.toSeq.grouped(1000).foreach { x =>
      holderPersistentAPI.updateTokenUriBulk(x)
    }
    (totalTokenURIs.size, inventorySize)
  }
  override def run(args: Array[String]): Unit = {
    val cnt = 10000
    11478L to 15350L foreach { x =>
      val t1 = System.currentTimeMillis()
      val (updated, invenCount) = tokenURI(x * cnt, cnt)
      try {
        SlackUtil.sendMessage(
          s"TokenURI: ${x * cnt} ~ ${x * cnt + cnt} is done. ${System
            .currentTimeMillis() - t1} ms, ${chainPhase}, ${updated} updated. / ${invenCount} total"
        )
      } catch {
        case e: Exception => {
          e.printStackTrace()
        }
      }
    }
  }
}
