package io.klaytn.utils.s3

import com.amazonaws.auth.{
  AWSStaticCredentialsProvider,
  BasicAWSCredentials,
  EC2ContainerCredentialsProviderWrapper
}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import io.klaytn.utils.Utils
import io.klaytn.utils.spark.UserConfig

import java.io.{InputStreamReader, Reader}
import scala.collection.mutable
import scala.io.Source
import scala.jdk.CollectionConverters._
import scala.util.Try

object S3Util {
  private val s3Client: AmazonS3 = {
    if (UserConfig.s3AccessKey.isSuccess && UserConfig.s3SecretKey.isSuccess) {
      AmazonS3ClientBuilder
        .standard()
        .withCredentials(
          new AWSStaticCredentialsProvider(
            new BasicAWSCredentials(UserConfig.s3AccessKey.get,
                                    UserConfig.s3SecretKey.get)))
        .build()
    } else {
      AmazonS3ClientBuilder
        .standard()
        .withCredentials(new EC2ContainerCredentialsProviderWrapper)
        .build()
    }
  }

  def writeText(bucket: String, key: String, content: String): Unit = {
    Utils.retry(10, 50)(s3Client.putObject(bucket, key, content))
  }

  def readText(bucket: String, key: String): Option[String] = {
    Try(
      Some(
        Utils.retry(10, 50)(
          Source
            .fromInputStream(s3Client.getObject(bucket, key).getObjectContent)
            .getLines()
            .mkString)))
      .getOrElse(None)
  }

  def getReader(bucket: String, key: String): Reader = {
    new InputStreamReader(
      s3Client
        .getObject(bucket, key)
        .getObjectContent)
  }

  def delete(bucket: String, key: String, recursive: Boolean): Unit = {
    if (recursive) {
      list(bucket, key).foreach { key1 =>
        Utils.retry(10, 50)(s3Client.deleteObject(bucket, key1))
      }
    } else {
      Utils.retry(10, 50)(s3Client.deleteObject(bucket, key))
    }
  }

  def exist(bucket: String, key: String): Boolean = {
    Utils.retry(10, 50)(s3Client.doesObjectExist(bucket, key))
  }

  /**
    * @return The path with the prefix is returned.
    *         If your data looks like this
    *         s3://bucket/prefix/object1
    *         s3://bucket/prefix/object2
    *         s3://bucket/prefix/object3/obejct4
    *         Return Data will be Seq("prefix/object1", "prefix/object2", "prefix/object3/object4")
    */
  def list(bucket: String, prefix: String): Seq[String] = {
    val result = mutable.ArrayBuffer.empty[String]
    var listing = s3Client.listObjects(bucket, prefix)
    result.appendAll(listing.getObjectSummaries.asScala.map(_.getKey))

    while (listing.isTruncated) {
      listing = s3Client.listNextBatchOfObjects(listing)
      result.appendAll(listing.getObjectSummaries.asScala.map(_.getKey))
    }

    result
  }

  def copy(sourceBucket: String,
           sourceKey: String,
           destinationBucket: String,
           destinationKey: String): Unit = {
    s3Client.copyObject(sourceBucket,
                        sourceKey,
                        destinationBucket,
                        destinationKey)
  }
}
