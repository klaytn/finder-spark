package io.klaytn.client.es.request.impl

import io.klaytn.client.es.request.ESUpdateRequest
import io.klaytn.utils.JsonUtil

case class ESUpdateRequestImpl(doc: Map[String, Any]) extends ESUpdateRequest {
  override def toRequestBodyString(): String = {
    val value = JsonUtil.asJsonAny(doc)
    s"""{
       |  "doc": $value 
       |}""".stripMargin
  }
}
