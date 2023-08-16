package io.klaytn.utils.http

import io.klaytn.utils.DateUtil
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.{
  HttpDelete,
  HttpGet,
  HttpHead,
  HttpPost,
  HttpPut,
  HttpRequestBase
}
import org.apache.http.conn.ConnectTimeoutException
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{
  BasicCredentialsProvider,
  CloseableHttpClient,
  HttpClientBuilder
}
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.util.EntityUtils

import java.net.SocketTimeoutException
import java.nio.charset.StandardCharsets

class ApacheHttpClient(
    connectTimeout: Int = 5000,
    socketTimeout: Int = 3000,
    maxConnectionSize: Option[Int] = None,
    basicAuth: Option[HttpClient.BasicAuth] = None
) extends HttpClient {
  @transient private lazy val httpClient: CloseableHttpClient =
    createHttpClient()

  def get(url: String,
          headers: Map[String, String] = Map.empty): HttpClient.Response = {
    val httpRequest = setHeaders(new HttpGet(url), headers)
    execute(httpRequest)
  }

  def post(
      url: String,
      data: String,
      headers: Map[String, String] = Map.empty
  ): HttpClient.Response = {
    val httpRequest = {
      val request = new HttpPost(url)
      val body = new StringEntity(data, StandardCharsets.UTF_8)
      request.setEntity(body)
      setHeaders(request, headers)
    }
    execute(httpRequest)
  }

  def put(
      url: String,
      data: String,
      headers: Map[String, String] = Map.empty
  ): HttpClient.Response = {
    val httpRequest = {
      val request = new HttpPut(url)
      val body = new StringEntity(data, StandardCharsets.UTF_8)
      request.setEntity(body)
      setHeaders(request, headers)
    }
    execute(httpRequest)
  }

  def delete(
      url: String,
      data: String,
      headers: Map[String, String] = Map.empty
  ): HttpClient.Response = {
    assert(data != null || data.trim != "",
           "apache http client disallows 'entity body' in delete method")
    val httpRequest = setHeaders(new HttpDelete(url), headers)
    execute(httpRequest)
  }

  def head(url: String, headers: Map[String, String]): HttpClient.Response = {
    val httpRequest = setHeaders(new HttpHead(url), headers)
    execute(httpRequest)
  }

  private def createHttpClient(): CloseableHttpClient = {
    val requestConfig = RequestConfig.custom
      .setConnectTimeout(connectTimeout)
      .setSocketTimeout(socketTimeout)
      .build
    val builder = HttpClientBuilder
      .create()
      .setDefaultRequestConfig(requestConfig)

    // connection pool
    val connectionManager = new PoolingHttpClientConnectionManager
    maxConnectionSize.foreach { size =>
      connectionManager.setMaxTotal(size)
    }
    builder.setConnectionManager(connectionManager)

    // basic credential
    basicAuth.foreach { auth: HttpClient.BasicAuth =>
      val provider = new BasicCredentialsProvider()
      val credentials =
        new UsernamePasswordCredentials(auth.username, auth.password)
      provider.setCredentials(AuthScope.ANY, credentials)
      builder.setDefaultCredentialsProvider(provider)
    }

    builder.build()
  }

  protected def setHeaders(request: HttpRequestBase,
                           headers: Map[String, String]): HttpRequestBase = {
    headers.foreach {
      case (key, value) => request.setHeader(key, value)
    }
    request
  }

  protected def execute(httpRequest: HttpRequestBase): HttpClient.Response = {
    try {
      val httpResponse = httpClient.execute(httpRequest)
      val httpEntity = httpResponse.getEntity
      val statusCode = httpResponse.getStatusLine.getStatusCode

      val response = if (httpEntity == null) {
        HttpClient.Response(statusCode, "")
      } else {
        val responseBody =
          EntityUtils.toString(httpEntity, StandardCharsets.UTF_8)
        HttpClient.Response(statusCode, responseBody)
      }
      EntityUtils.consumeQuietly(httpEntity)
      response

    } catch {
      case e: SocketTimeoutException =>
        System.err.println(
          s"${DateUtil.nowKST().toString("yyyyMMdd HH:mm:ss")} - ${e.getLocalizedMessage}, (socketTimeout: $socketTimeout, url: ${httpRequest.getURI.toString})"
        )
        throw e
      case e: ConnectTimeoutException =>
        System.err.println(
          s"${DateUtil.nowKST().toString("yyyyMMdd HH:mm:ss")} - ${e.getLocalizedMessage}, (connectTimeout: $connectTimeout, url: ${httpRequest.getURI.toString}"
        )
        throw e
      case e: Throwable =>
        throw e
    }
  }
}
