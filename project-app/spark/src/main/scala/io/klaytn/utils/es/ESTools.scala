package io.klaytn.utils.es

import io.klaytn.client.es.ESConfig
import io.klaytn.utils.http.HttpClient
import org.apache.http.HttpHeaders
import io.klaytn._

class ESTools(config: ESConfig, httpClient: LazyEval[HttpClient]) {
  private val fixedHeader = Map(HttpHeaders.CONTENT_TYPE -> "application/json")

  def getRemotePipeline(): HttpClient.Response = {
    httpClient.get(config.remotePipelineUrl(), headers = fixedHeader)
  }

  def getRemoteTemplate(): HttpClient.Response = {
    httpClient.get(config.remoteTemplateUrl(), headers = fixedHeader)
  }

  def registerRemotePipeline(): HttpClient.Response = {
    val pipeline = config.localPipeline.get
    println(s"""<pipeline>
               |$pipeline
               |""".stripMargin)
    httpClient.put(config.remotePipelineUrl(), pipeline, headers = fixedHeader)
  }

  def registerRemoteTemplate(): HttpClient.Response = {
    val template = config.localTemplate.get
    println(s"""<template>
               |$template
               |""".stripMargin)
    httpClient.put(config.remoteTemplateUrl(), template, headers = fixedHeader)
  }

  def removeTemplate(): Unit = {
    val requestURL = s"${config.url}/_template/${config.indexName}"
    httpClient.delete(requestURL, "", headers = fixedHeader)
  }

  def removePipeline(): Unit = {
    val requestURL = s"${config.url}/_ingest/pipeline/${config.indexName}"
    httpClient.delete(requestURL, "", headers = fixedHeader)
  }
}
