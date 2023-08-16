package io.klaytn.apps.restore.holder

import io.klaytn.utils.spark.SparkHelper

/*
--driver-memory 10g
--num-executors 2
--executor-cores 4
--executor-memory 3g
--conf spark.app.phase=prod-cypress-modify-me
--class io.klaytn.apps.restore.holder.MinusHolderBatch
 */
object MinusHolderBatch extends SparkHelper {
  import MinusHolderBatchDeps._

  def token(): Unit = {
    sc.parallelize(Seq(1))
      .repartition(1)
      .flatMap(_ => service.getMinusTokenHolders())
      .repartition(8)
      .foreach {
        case (contractAddress, holderAddress) =>
          service.modifyMinusTokenHolder(contractAddress, holderAddress)
      }
  }

  def kip17(): Unit = {
    sc.parallelize(Seq(1))
      .repartition(1)
      .flatMap(_ => service.getMinusKIP17Holders())
      .repartition(8)
      .foreach {
        case (contractAddress, holderAddress) =>
          service.modifyMinusKIP17Holder(contractAddress, holderAddress)
      }
  }

  def kip37(): Unit = {
    sc.parallelize(Seq(1))
      .repartition(1)
      .flatMap(_ => service.getMinusKIP37Holders())
      .repartition(8)
      .foreach {
        case (contractAddress, holderAddress, tokenId) =>
          service.modifyMinusKIP37Holder(contractAddress,
                                         holderAddress,
                                         tokenId)
      }
  }

  override def run(args: Array[String]): Unit = {
    token()
    kip17()
    kip37()
  }
}
