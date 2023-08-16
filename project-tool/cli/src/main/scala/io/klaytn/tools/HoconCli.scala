package io.klaytn.tools

import com.typesafe.config.ConfigFactory
import scala.util.{Failure, Success, Try}

object HoconCli {
  def usage(): Unit = {
    val msg =
      """
        |Usage: HoconCli {conf file path} {key}
        |
        |ex) HoconCli io/klaytn/apps/account/AccountToDBStreaming-prod-baobab.conf spark.app.batchDuration
        |
        |""".stripMargin
    System.err.println(msg)
  }
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      usage()
      System.exit(-1)
    }
    val confPath = args(0)
    val key = args(1)
    val conf = ConfigFactory.load(confPath)
    Try(conf.getString(key)) match {
      case Success(value) => println(value)
      case Failure(_) =>
        System.err.println(s"$key is not found")
        System.exit(1)
    }
  }
}
