package io.klaytn.apps.block

import io.klaytn.client.es.{ESClientImpl, ESConfig}
import io.klaytn.utils.http.HttpClient

object BlockToESStreamingDeps {
  private val esConfig = ESConfig.transaction()
  private val httpClient = HttpClient.createPooled()
  private val esClient = new ESClientImpl(esConfig, httpClient)
  private val errorReporter = new BlockToESErrorReporter()
  val processor = new BlockToESProcessor(esClient, errorReporter)
}
