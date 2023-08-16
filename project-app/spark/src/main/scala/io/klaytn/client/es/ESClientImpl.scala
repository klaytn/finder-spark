package io.klaytn.client.es

import io.klaytn._
import io.klaytn.client.es.ESClient.ESBulkIndexResponse
import io.klaytn.client.es.request.impl.{ESBulkIndexRequest, ESScriptRequest}
import io.klaytn.client.es.request.{ESQueryRequest, ESUpdateRequest}
import io.klaytn.utils.http.HttpClient
import org.apache.http.HttpHeaders

/**
  * Consider using the library if you need multiple operations in the future
  * https://opensearch.org/docs/latest/clients/java-rest-high-level/
  */
class ESClientImpl(config: ESConfig, httpClient: LazyEval[HttpClient])
    extends ESClient {
  override val indexName: String = config.indexName

  private val fixedHeaders = Map(HttpHeaders.CONTENT_TYPE -> "application/json")

  private val pipelineSuffix = if (config.usePipeline()) {
    s"?pipeline=${config.indexName}"
  } else {
    ""
  }

  override def bulkInsert(request: ESBulkIndexRequest): ESBulkIndexResponse = {
    val bulkUrl = s"${config.url}/${config.indexName}/_bulk$pipelineSuffix"
    val response = httpClient.post(bulkUrl,
                                   headers = fixedHeaders,
                                   data = request.toRequestBodyString())
    ESBulkIndexResponse(response)
  }

  override def insert(id: String, data: String): HttpClient.Response = {
    val url = s"${config.url}/${config.indexName}/_doc/$id$pipelineSuffix"
    httpClient.put(url, headers = fixedHeaders, data = data)
  }

  override def update(id: String,
                      request: ESUpdateRequest): HttpClient.Response = {
    val requestBody = request.toRequestBodyString()
    val url = s"${config.url}/${config.indexName}/_update/$id"
    httpClient.post(url, headers = fixedHeaders, data = requestBody)
  }

  override def updateByQuery(
      query: ESQueryRequest,
      updateRequest: ESScriptRequest): HttpClient.Response = {
    val requestBody = s"""{
                         |  ${query.toQueryString()},
                         |  ${updateRequest.toScriptString()}
                         |}
                         |""".stripMargin
    val url = s"${config.url}/${config.indexName}/_update_by_query"
    httpClient.post(url, headers = fixedHeaders, data = requestBody)
  }

  override def search(query: ESQueryRequest): HttpClient.Response = {
    val url = s"${config.url}/${config.indexName}/_search"
    httpClient.post(url,
                    headers = fixedHeaders,
                    data = query.toRequestBodyString())
  }

  override def get(id: String): HttpClient.Response = {
    val url = s"${config.url}/${config.indexName}/_doc/$id"
    httpClient.get(url, headers = fixedHeaders)
  }
}
