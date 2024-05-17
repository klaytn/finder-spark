package io.klaytn.utils.gcs

import io.klaytn.utils.Utils
import io.klaytn.utils.spark.UserConfig

import java.io.{InputStreamReader, Reader}
import scala.collection.mutable
import java.io.ByteArrayInputStream

import com.google.cloud.storage.{BlobId, BlobInfo, Storage, StorageOptions}
import com.google.cloud.storage.Storage.{BlobListOption, CopyRequest}

object GCSUtil {
  private val storage: Storage = {
    if (UserConfig.projectId.isSuccess) {
      StorageOptions
        .newBuilder()
        .setProjectId(UserConfig.projectId.get)
        .build()
        .getService();
    } else {
      StorageOptions.newBuilder().build().getService();
    }
  }

  def writeText(bucket: String, key: String, content: String): Unit = {
    val bucketInfo = BlobId.of(bucket, key)
    val blobInfo =
      BlobInfo.newBuilder(bucketInfo).setContentType("text/plain").build()
    Utils.retry(10, 50)(storage.create(blobInfo, content.getBytes()))
  }

  def readText(bucket: String, key: String): Option[String] = {
    val blobId = BlobId.of(bucket, key);
    val blob = storage.get(blobId);
    if (blob != null) {
      val bytes = blob.getContent() // Array[Byte]
      Some(new String(bytes))
    } else {
      None
    }
  }

  def getReader(bucket: String, key: String): Reader = {
    val blobId = BlobId.of(bucket, key);
    val blob = storage.get(blobId);
    if (blob != null) {
      val bytes = blob.getContent() // Array[Byte]
      new InputStreamReader(new ByteArrayInputStream(bytes))
    } else {
      null
    }
  }
  def delete(bucket: String, key: String, recursive: Boolean): Unit = {
    if (recursive) {
      list(bucket, key).foreach { key1 =>
        Utils.retry(10, 50)(storage.delete(bucket, key1))
      }
    } else {
      Utils.retry(10, 50)(storage.delete(bucket, key))
    }
  }

  def exist(bucket: String, key: String): Boolean = {
    Utils.retry(10, 50)(storage.get(bucket, key) != null)
  }

  /**
    * @return The path with the prefix is returned.
    *         If your data looks like this
    *         gs://bucket/prefix1/prefix2/file1
    *         gs://bucket/prefix1/prefix2/file2
    *          gs://bucket/prefix1/prefix2/file3
    * Then, list("bucket", "prefix1/prefix2") returns
    * Seq("prefix1/prefix2/file1", "prefix1/prefix2/file2", "prefix1/prefix2/file3")
    */
  def list(bucket: String, prefix: String): Seq[String] = {
    val blobList = storage.list(bucket, BlobListOption.prefix(prefix))
    val blobListIterator = blobList.iterateAll().iterator()
    val result = mutable.ArrayBuffer[String]()
    while (blobListIterator.hasNext()) {
      val blob = blobListIterator.next()
      result += blob.getName()
    }
    result
  }

  def copy(sourceBucket: String,
           sourceKey: String,
           destinationBucket: String,
           destinationKey: String): Unit = {
    val sourceBlobId = BlobId.of(sourceBucket, sourceKey);
    val targetBlobId = BlobId.of(destinationBucket, destinationKey);
    val targetBlob = storage.get(targetBlobId);

    Utils.retry(10, 50)(
      CopyRequest
        .newBuilder()
        .setSource(sourceBucket, sourceKey)
        .setTarget(targetBlob)
        .build()
    )
  }

  def download(bucket: String, key: String, localPath: String): Unit = {
    val blobId = BlobId.of(bucket, key);
    val path = java.nio.file.Paths.get(localPath);
    val blob = storage.get(blobId);
    if (blob != null) {
      blob.downloadTo(path)
    }
  }
}
