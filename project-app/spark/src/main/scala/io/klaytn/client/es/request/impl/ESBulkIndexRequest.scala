package io.klaytn.client.es.request.impl

import io.klaytn.client.es.request.ESRestApiRequest

case class ESBulkIndexRequest(indexName: String,
                              indexRequestList: Seq[ESIndexRequest])
    extends ESRestApiRequest {
  override def toRequestBodyString(): String = {
    val requestBody = indexRequestList
      .map { request =>
        val docId = request.docId
        val data = request.data

        val metadata =
          s"""{"index": {"_index": "$indexName", "_id": "$docId"}}"""
        s"""$metadata
           |$data
           |""".stripMargin
      }
      .mkString("\n")

    requestBody
  }
}
