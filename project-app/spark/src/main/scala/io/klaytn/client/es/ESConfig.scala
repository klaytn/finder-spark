package io.klaytn.client.es

import com.typesafe.config.{Config, ConfigFactory}
import io.klaytn.client.es.ESConfig.BasicAuth
import io.klaytn.utils.Jinja

import scala.io.Source
import scala.util.Try

object ESConfig {
  case class BasicAuth(id: String, password: String)

  def transaction(): ESConfig = apply("spark.app.es.transaction_v1")

  def account(): ESConfig = apply("spark.app.es.account_v1")

  def contract(): ESConfig = apply("spark.app.es.contract_v1")

  def apply(config: Config): ESConfig = {
    ESConfig(
      url = config.getString("url"),
      indexName = config.getString("index_name"),
      numberOfShards = config.getInt("number_of_shard"),
      numberOfReplicas = config.getInt("number_of_replica"),
      auth = (Try { config.getString("username") }.toOption, Try {
        config.getString("password")
      }.toOption) match {
        case (Some(username), Some(password)) =>
          Some(BasicAuth(username, password))
        case _ =>
          None
      },
      templatePath = Try { config.getString("template_file_path") }.toOption,
      pipelinePath = Try { config.getString("pipeline_file_path") }.toOption
    )
  }

  private def apply(configName: String): ESConfig =
    apply(ConfigFactory.load().getConfig(configName))
}

case class ESConfig(
    url: String,
    indexName: String,
    numberOfShards: Int,
    numberOfReplicas: Int,
    auth: Option[BasicAuth],
    templatePath: Option[String],
    pipelinePath: Option[String],
) {
  def remoteTemplateUrl(): String = s"$url/_template/$indexName?pretty"
  def remotePipelineUrl(): String = s"$url/_ingest/pipeline/$indexName?pretty"
  def usePipeline(): Boolean = localPipeline.isDefined

  lazy val localTemplate: Option[String] = templatePath
    .map(this.getResourceAsString)
    .map { template =>
      val bindings = Map("index_name" -> indexName,
                         "number_of_shards" -> numberOfShards.toString,
                         "number_of_replicas" -> numberOfReplicas.toString)
      Jinja.render(template, bindings)
    }

  lazy val localPipeline: Option[String] = pipelinePath
    .map(this.getResourceAsString)
    .map { pipeline =>
      val bindings = Map("index_name" -> indexName)
      Jinja.render(pipeline, bindings)
    }

  private def getResourceAsString(path: String): String = {
    if (path == "") {
      throw new IllegalArgumentException(s"missing path: $path")
    }
    val stream = getClass.getClassLoader.getResourceAsStream(path)
    if (stream == null) {
      throw new IllegalStateException(s"cannot find index template file: $path")
    }
    val bufferedSource = Source.fromInputStream(stream)
    bufferedSource.getLines().mkString("\n")
  }
}
