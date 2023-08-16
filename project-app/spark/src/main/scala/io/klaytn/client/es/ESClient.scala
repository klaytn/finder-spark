package io.klaytn.client.es

import io.klaytn.client.es.ESClient.ESBulkIndexResponse
import io.klaytn.client.es.request.impl.{ESBulkIndexRequest, ESScriptRequest}
import io.klaytn.client.es.request.{ESQueryRequest, ESUpdateRequest}
import io.klaytn.utils.JsonUtil
import io.klaytn.utils.http.HttpClient

object ESClient {
  case class ESBulkIndexResponse(status: Int, body: String) {
    def isSuccess(): Boolean = !isFailed()

    def isFailed(): Boolean = {
      if (status.toString.head != '2') {
        return true
      }
      val map = JsonUtil.fromJsonObject(body).getOrElse(Map.empty)
      !map.contains("errors") || map("errors") == true
    }
  }
  object ESBulkIndexResponse {
    def apply(httpResponse: HttpClient.Response): ESBulkIndexResponse = {
      ESBulkIndexResponse(httpResponse.status, httpResponse.body)
    }
  }
}

trait ESClient extends Serializable {
  val indexName: String
  def bulkInsert(request: ESBulkIndexRequest): ESBulkIndexResponse
  def insert(id: String, data: String): HttpClient.Response
  def update(id: String, request: ESUpdateRequest): HttpClient.Response
  def updateByQuery(query: ESQueryRequest,
                    updateRequest: ESScriptRequest): HttpClient.Response
  def search(query: ESQueryRequest): HttpClient.Response
  def get(id: String): HttpClient.Response
}
