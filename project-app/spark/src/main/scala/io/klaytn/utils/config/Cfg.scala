package io.klaytn.utils.config

import com.typesafe.config.{Config, ConfigFactory}

import java.time.Duration
import scala.jdk.CollectionConverters.asScalaBufferConverter
import scala.util.Try

object Cfg extends Serializable {
//  val config: Config = {
//    Option(SparkEnv.get.conf.get("spark.app.phase")) match {
//      case Some(phase) =>
//        val configFile =
//          s"${SparkEnv.get.conf.get("spark.app.class.name").stripSuffix("$").replaceAll("[.]", "/")}-$phase.conf"
//        ConfigFactory.load(configFile)
//      case _ =>
//        ConfigFactory.load()
//    }
//  }
  lazy val config: Config = {
    Option(System.getProperty("custom.config.path")) match {
      case Some(resource) => ConfigFactory.load(resource)
      case _              => ConfigFactory.load()
    }
  }

  def hasPath(path: String): Boolean = config.hasPath(path)
  def getString(path: String): String = config.getString(path)
  def getBoolean(path: String): Boolean = config.getBoolean(path)
  def getNumber(path: String): Number = config.getNumber(path)
  def getInt(path: String): Int = config.getInt(path)
  def getStringList(path: String): Seq[String] =
    config.getStringList(path).asScala
  def getDuration(path: String): Duration = config.getDuration(path)

  def findString(path: String): Option[String] =
    Try { getString(path) }.toOption
}
