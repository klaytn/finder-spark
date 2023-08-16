package io.klaytn.utils.http

class DefaultHttpClient(readTimeout: Int, connectTimeout: Int)
    extends HttpClient {

  def get(
      url: String,
      headers: Map[String, String] = Map.empty
  ): HttpClient.Response = {
    val response =
      requests.get(url,
                   headers = headers,
                   readTimeout = readTimeout,
                   connectTimeout = connectTimeout)
    HttpClient.Response(response.statusCode, response.text())
  }

  def post(
      url: String,
      data: String = "",
      headers: Map[String, String] = Map.empty
  ): HttpClient.Response = {
    val response =
      requests
        .post(url,
              headers = headers,
              data = data,
              readTimeout = readTimeout,
              connectTimeout = connectTimeout)
    HttpClient.Response(response.statusCode, response.text())
  }

  def put(
      url: String,
      data: String = "",
      headers: Map[String, String] = Map.empty
  ): HttpClient.Response = {
    val response =
      requests
        .put(url,
             headers = headers,
             data = data,
             readTimeout = readTimeout,
             connectTimeout = connectTimeout)
    HttpClient.Response(response.statusCode, response.text())
  }

  def delete(
      url: String,
      data: String = "",
      headers: Map[String, String] = Map.empty
  ): HttpClient.Response = {
    val response =
      requests
        .delete(url,
                headers = headers,
                data = data,
                readTimeout = readTimeout,
                connectTimeout = connectTimeout)
    HttpClient.Response(response.statusCode, response.text())
  }

  def head(url: String, headers: Map[String, String]): HttpClient.Response = {
    val response =
      requests.head(url,
                    headers = headers,
                    readTimeout = readTimeout,
                    connectTimeout = connectTimeout)
    HttpClient.Response(response.statusCode, response.text())
  }
}
