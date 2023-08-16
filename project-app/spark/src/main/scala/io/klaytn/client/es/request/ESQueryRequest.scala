package io.klaytn.client.es.request

trait ESQueryRequest extends ESRestApiRequest {
  def toQueryString(): String

  override def toRequestBodyString(): String = {
    s"""{
       |    ${toQueryString()}
       |}
       |""".stripMargin
  }
}
