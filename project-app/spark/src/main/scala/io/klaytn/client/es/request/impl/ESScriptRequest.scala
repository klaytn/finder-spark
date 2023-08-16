package io.klaytn.client.es.request.impl

import io.klaytn.client.es.request.ESUpdateRequest

case class ESScriptRequest(values: Seq[ESScriptRequestValue])
    extends ESUpdateRequest {
  def toScriptString(): String = {
    val source = values.map(_.toSourceString()).mkString(";")
    val params = values.map(_.toParamString()).mkString(",\n")
    s""""script": {
       |  "source": "$source",
       |  "lang": "painless",
       |  "params": {
       |    $params
       |  }
       |}
       |""".stripMargin
  }

  override def toRequestBodyString(): String = {
    s"""{
       |  ${toScriptString()} 
       |}""".stripMargin
  }
}

object ESScriptRequest {
  def apply(value: ESScriptRequestValue): ESScriptRequest = {
    new ESScriptRequest(Seq(value))
  }
}
