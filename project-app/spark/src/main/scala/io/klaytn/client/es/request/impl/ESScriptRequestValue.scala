package io.klaytn.client.es.request.impl

sealed trait ESScriptRequestValue {
  def toSourceString(): String
  def toParamString(): String
}

case class IncrementScriptValue(key: String, value: Long)
    extends ESScriptRequestValue {
  override def toSourceString(): String = {
    s"ctx._source.$key = ctx._source.$key + params.$key"
  }

  override def toParamString(): String = {
    s""""$key": $value"""
  }
}

case class AssignScriptValue(key: String, value: String)
    extends ESScriptRequestValue {
  override def toSourceString(): String = {
    s"ctx._source.$key = params.$key"
  }

  override def toParamString(): String = {
    s""""$key": $value"""
  }
}
object AssignScriptValue {
  def apply(key: String, value: Long): AssignScriptValue = {
    new AssignScriptValue(key, s"$value")
  }
  def apply(key: String, value: String): AssignScriptValue = {
    new AssignScriptValue(key, s""""$value"""")
  }
}
