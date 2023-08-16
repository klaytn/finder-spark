package io.klaytn.client.es.request

import io.klaytn.client.es.request.impl.ESScriptRequest

case class ESUpdateByQueryRequest(query: ESQueryRequest,
                                  update: ESScriptRequest)
    extends ESRestApiRequest {
  override def toRequestBodyString(): String = {
    s"""{
       |  ${query.toQueryString()},
       |  ${update.toScriptString()}
       |}
       |""".stripMargin
  }
}
