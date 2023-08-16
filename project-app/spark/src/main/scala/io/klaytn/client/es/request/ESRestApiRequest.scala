package io.klaytn.client.es.request

trait ESRestApiRequest {
  def toRequestBodyString(): String
  override def toString: String = toRequestBodyString()
}
