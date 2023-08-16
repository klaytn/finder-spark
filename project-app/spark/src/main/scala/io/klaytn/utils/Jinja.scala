package io.klaytn.utils

import com.hubspot.jinjava.Jinjava
import scala.collection.JavaConverters._

object Jinja {
  private lazy val jinja = new Jinjava()

  def render(template: String, bindings: Map[String, String]): String = {
    jinja.render(template, bindings.asJava)
  }
}
