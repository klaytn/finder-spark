package io.klaytn.tools.model

//noinspection TypeAnnotation
object ServiceType extends Enumeration {
  val klaytn = Value("klaytn")

  private val byLowerString = values.map(x => (x.toString.toLowerCase, x)).toMap

  def of(value: String): Option[ServiceType.Value] =
    Option(value).map(_.toLowerCase).flatMap(byLowerString.get)
}
