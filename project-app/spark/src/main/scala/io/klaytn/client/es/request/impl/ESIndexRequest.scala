package io.klaytn.client.es.request.impl

import io.klaytn.client.es.request.ESRestApiRequest

case class ESIndexRequest(docId: String, data: String)
    extends ESRestApiRequest {
  override def toRequestBodyString(): String = data
}
