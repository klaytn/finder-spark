package io.klaytn.utils

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.{
  DefaultScalaModule,
  ScalaObjectMapper
}

object ObjectMapperUtil {
  val objectMapper = new ObjectMapper() with ScalaObjectMapper
  objectMapper.registerModule(DefaultScalaModule)
//  objectMapper.setSerializationInclusion(Include.NON_NULL)

  def asJson[T](x: T): String = objectMapper.writeValueAsString(x)
}
