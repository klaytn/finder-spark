package io.klaytn.model

import com.typesafe.config.Config

import scala.util.Try

//noinspection TypeAnnotation
object Phase extends Enumeration {
  val local = Value("local")
  val dev = Value("dev")
  val prod = Value("prod")

  private val byLowercaseString =
    values.map(x => (x.toString.toLowerCase(), x)).toMap

  def get(config: Config): Phase.Value = {
    val phaseOpt = Try {
      config.getString("spark.app.phase")
    }.toOption.flatMap(of)

    if (phaseOpt.isEmpty) {
      throw new IllegalStateException("missing phase")
    }
    phaseOpt.get
  }

  def of(value: String): Option[Phase.Value] = {
    Option(value)
      .filter(_.nonEmpty)
      .map(_.toLowerCase)
      .filter(byLowercaseString.contains)
      .flatMap(byLowercaseString.get)
  }
}
