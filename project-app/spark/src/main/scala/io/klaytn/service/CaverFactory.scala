package io.klaytn.service

import com.klaytn.caver.Caver
import io.klaytn.utils.config.Cfg

import scala.util.Try

object CaverFactory {
  def getCaverUrl(configPath: String): String =
    Try(Cfg.getString(configPath)).getOrElse(null)

  val caverUrl: String = {
    val caverUrl = getCaverUrl("spark.app.caver.url")
    if (caverUrl == null)
      "http://cypress.en.klaytnfinder.io:8551" // "http://baobab.en.klaytnfinder.io:8551"
    else caverUrl
  }

  val caverUrl2: String = {
    val caverUrl = getCaverUrl("spark.app.caver2.url")
    if (caverUrl == null)
      "http://cypress.en.klaytnfinder.io:8551" // "http://baobab.en.klaytnfinder.io:8551"
    else caverUrl
  }

  val caver: Caver = new Caver(caverUrl)
  val caver2: Caver = new Caver(caverUrl2)
}
