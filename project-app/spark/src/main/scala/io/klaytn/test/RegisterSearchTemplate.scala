package io.klaytn.test

import io.klaytn.utils.es.ESTools
import io.klaytn.client.es.ESConfig
import io.klaytn.utils.http.HttpClient

object RegisterSearchTemplate extends SparkSessionWrapper {
  def main(args: Array[String]): Unit = {
    import spark.implicits._
    val httpClient = HttpClient.createPooled()
    val contractES = new ESTools(ESConfig.contract(), httpClient)
    val accountES = new ESTools(ESConfig.account(), httpClient)
    val result1 = contractES.registerRemoteTemplate()
    println(result1)
    val result2 = accountES.registerRemoteTemplate()
    println(result2)
  }
}
