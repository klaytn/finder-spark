package io.klaytn.client.es.request.impl

import io.klaytn.client.es.request.ESQueryRequest

object ESTermQueryRequest {
  def apply(key: String, value: Long): ESTermQueryRequest = {
    val keyValue = (s""""$key"""", value.toString)
    new ESTermQueryRequest(Seq(keyValue))
  }
  def apply(key: String, value: String): ESTermQueryRequest = {
    val keyValue = (s""""$key"""", s""""$value"""")
    new ESTermQueryRequest(Seq(keyValue))
  }
}

case class ESTermQueryRequest(keyValues: Seq[(String, String)])
    extends ESQueryRequest {
  private lazy val termExpression = keyValues
    .map {
      case (key, value) => s"$key: $value"
    }
    .mkString(",\n")

  override def toRequestBodyString(): String = {
    s"""{
       |  ${toQueryString()}
       |}""".stripMargin
  }

  override def toQueryString(): String = {
    s""""query": {
       |  "term": {
       |    $termExpression
       |  }
       |}
       |""".stripMargin
  }
}
