package io.klaytn.tools

import com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import JavaConverters._

object Test {
  def main(args: Array[String]): Unit = {
    val s3Client = AmazonS3ClientBuilder
      .standard()
      .withCredentials(new EC2ContainerCredentialsProviderWrapper)
      .build()
    s3Client
      .listObjects("klaytn-prod-spark", "jobs")
      .getObjectSummaries
      .asScala
      .map(_.getKey)
      .foreach(println)
  }
}
