package io.klaytn.apps.block

import io.klaytn._
import io.klaytn.client.es.ESClient
import io.klaytn.client.es.request.impl.{ESBulkIndexRequest, ESIndexRequest}
import io.klaytn.model.Block
import io.klaytn.model.finder.es.ESTransaction
import io.klaytn.utils.Utils

class BlockToESProcessor(esClient: LazyEval[ESClient],
                         errorReporter: LazyEval[BlockToESErrorReporter])
    extends Serializable {
  private val esRetryCount = 3
  private val esRetrySleepMS = 500

  def process(block: Block): Unit = {
    Utils.retryAndReportOnFail(esRetryCount,
                               esRetrySleepMS,
                               errorReporter.report) {
      _process(block)
    }
  }

  private def _process(block: Block): Unit = {
    val indexRequestList = block.toRefined._2
      .map(ESTransaction(_))
      .map { x =>
        val docId = x.transaction_hash
        val doc = x.asJson
        ESIndexRequest(docId, doc)
      }
    if (indexRequestList.isEmpty) {
      return
    }
    // todo chunking
    val bulkRequest = ESBulkIndexRequest(esClient.indexName, indexRequestList)
    val response = esClient.bulkInsert(bulkRequest)
    if (response.isFailed()) {
      val message =
        s"""request: ${bulkRequest.toRequestBodyString()}
           |response: ${response.body}
           |""".stripMargin
      throw new IllegalStateException(message)
    }
  }
}
