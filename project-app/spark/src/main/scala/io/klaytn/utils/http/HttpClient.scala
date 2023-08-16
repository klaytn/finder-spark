package io.klaytn.utils.http

import com.typesafe.config.Config
import io.klaytn.utils.config.Cfg

import java.net.URLEncoder
import scala.util.Try

object HttpClient {
  case class BasicAuth(username: String, password: String)
  case class Response(status: Int, body: String) {
    def is2xx: Boolean = status.toString.head == '2'
  }

  def create(): DefaultHttpClient = create(Cfg.config)

  def create(config: Config): DefaultHttpClient = {
    val connectTimeout = Try { config.getInt("spark.app.http.connect_timeout") }
      .getOrElse(3000)
    val readTimeout = Try { config.getInt("spark.app.http.read_timeout") }
      .getOrElse(3000)

    create(readTimeout, connectTimeout)
  }

  def create(readTimeout: Int, connectTimeout: Int): DefaultHttpClient = {
    new DefaultHttpClient(readTimeout, connectTimeout)
  }

  def createPooled(
      connectTimeout: Int = 5000,
      socketTimeout: Int = 3000,
      maxConnectionSize: Option[Int] = None,
      basicAuth: Option[HttpClient.BasicAuth] = None
  ): HttpClient = {
    new ApacheHttpClient(
      connectTimeout,
      socketTimeout,
      maxConnectionSize,
      basicAuth
    )
  }
}

trait HttpClient extends Serializable {
  protected def encodeUrl(v: String): String = {
    URLEncoder.encode(v, "UTF-8")
  }

  def get(
      url: String,
      headers: Map[String, String] = Map.empty
  ): HttpClient.Response

  def post(
      url: String,
      data: String,
      headers: Map[String, String] = Map.empty
  ): HttpClient.Response

  def put(
      url: String,
      data: String,
      headers: Map[String, String] = Map.empty
  ): HttpClient.Response

  def delete(
      url: String,
      data: String,
      headers: Map[String, String] = Map.empty
  ): HttpClient.Response

  def head(url: String,
           headers: Map[String, String] = Map.empty): HttpClient.Response
}
