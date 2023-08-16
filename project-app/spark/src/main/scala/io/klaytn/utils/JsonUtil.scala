package io.klaytn.utils

import io.circe._
import io.circe.generic.AutoDerivation
import io.circe.parser._
import io.circe.syntax._

import scala.reflect.ClassTag
import scala.util.Try

object JsonUtil {
  implicit class OptionOps[A](fa: Option[A]) {
    def asTry[B](implicit ct: ClassTag[B]): Option[B] = fa match {
      case Some(v) if ct.runtimeClass.isInstance(v) => Some(v.asInstanceOf[B])
      case _                                        => None
    }
    def mapTry[B](f: A => B): Option[B] = fa.flatMap(x => Try(f(x)).toOption)
  }

  object Implicits extends AutoDerivation

  def asJson[T](x: T)(implicit encoder: Encoder[T]): String = {
    x.asJson.noSpaces
  }

  def asJsonAny(x: Any, spaces: Int = 0): String =
    processSpaces(scalaToJson(x), spaces)

  def fromJson[T](json: String)(implicit decoder: Decoder[T]): Option[T] = {
    decode[T](json) match {
      case Right(x) => Some(x)
      case Left(error) => {
//        println(s"### error: ${error}")
//        println(s"${json}")
//        SlackUtil.sendMessage(s"parse json error: ${json.split(",")(0)}")
        None
      }
    }
  }

  def fromJsonObject(json: String): Option[Map[String, Any]] = {
    decode[Json](json) match {
      case Right(x) => jsonToScala(x).asTry[Map[String, Any]]
      case Left(_)  => None
    }
  }

  def fromJsonArray(json: String): Option[Seq[Any]] = {
    decode[Json](json) match {
      case Right(x) => jsonToScala(x).asTry[Seq[Any]]
      case Left(_)  => None
    }
  }

  private def jsonToScala(json: Json): Option[Any] = {
    if (json.isNull) Some(null)
    else if (json.isBoolean) json.asBoolean
    else if (json.isString) json.asString
    else if (json.isNumber) json.asNumber.flatMap(_.toBigDecimal)
    else if (json.isArray)
      for {
        jsonArray <- json.asArray
        scalaSeq <- optionSequence(jsonArray.map(jsonToScala))
      } yield scalaSeq
    else if (json.isObject)
      for {
        jsonObject <- json.asObject
        scalaMap <- optionSequence(
          jsonObject.keys
            .map(
              key =>
                for {
                  nextJson <- jsonObject(key)
                  value <- jsonToScala(nextJson)
                } yield (key, value)
            )
            .toSeq
        )
      } yield scalaMap.filter(_._2 != null).toMap
    else None
  }

  @inline
  private def processSpaces(json: Json, spaces: Int) =
    if (spaces == 2) json.spaces2
    else if (spaces == 4) json.spaces4
    else json.noSpaces

  private def scalaToJson(any: Any): Json =
    any match {
      case null          => Json.Null
      case None          => Json.Null
      case Some(a)       => scalaToJson(a)
      case a: String     => Json.fromString(a)
      case a: Long       => Json.fromLong(a)
      case a: Int        => Json.fromInt(a)
      case a: Double     => Json.fromDoubleOrNull(a)
      case a: Float      => Json.fromFloatOrNull(a)
      case a: BigInt     => Json.fromBigInt(a)
      case a: BigDecimal => Json.fromBigDecimal(a)
      case a: Boolean    => Json.fromBoolean(a)
      case a: Seq[_]     => Json.fromValues(a.map(scalaToJson))
      case a: Map[_, _] =>
        Json.fromFields(
          a.map(kv => (kv._1.asInstanceOf[String], scalaToJson(kv._2)))
            .filter(_._2 != Json.Null))
      case a => Json.fromString(a.toString)
    }

  private def optionSequence[A](optionValues: Seq[Option[A]]): Option[Seq[A]] =
    optionValues.foldLeft(Option(Seq.empty[A]))((_optionValues, accNewValues) =>
      _optionValues.flatMap(sa => accNewValues.map(sa :+ _)))
}
