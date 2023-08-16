package io.klaytn.model

import com.typesafe.config.Config

import scala.util.Try

//noinspection TypeAnnotation
object Chain extends Enumeration {
  val local = Value("local")
  val baobab = Value("baobab")
  val cypress = Value("cypress")

  private val byLowercaseString =
    values.map(x => (x.toString.toLowerCase, x)).toMap

  def of(value: String): Option[Chain.Value] = {
    Option(value)
      .map(_.toLowerCase)
      .filter(_.nonEmpty)
      .filter(byLowercaseString.contains)
      .flatMap(byLowercaseString.get)
  }

  def get(config: Config): Chain.Value = {
    val chainOpt = Try {
      config.getString("spark.app.chain")
    }.toOption.flatMap(of)
    if (chainOpt.isEmpty) {
      throw new IllegalStateException("missing chain")
    }
    chainOpt.get
  }
}
