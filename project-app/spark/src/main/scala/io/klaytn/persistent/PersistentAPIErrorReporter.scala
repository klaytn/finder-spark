package io.klaytn.persistent

import io.klaytn.utils.SlackUtil
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.exception.ExceptionUtils

trait PersistentAPIErrorReporter extends Serializable {
  def report(e: Throwable): Unit
}

class DefaultPersistentAPIErrorReporter(resourceName: String)
    extends PersistentAPIErrorReporter {
  override def report(e: Throwable): Unit = {
    // TODO- Ignore Until migration
    if (!e.getMessage.contains("document_missing_exception")) {
      val message =
        s"""class: $resourceName
           |errorMessage: ${e.getMessage}
           |stackTrace: ${StringUtils.abbreviate(
             ExceptionUtils.getStackTrace(e),
             800)}
           |""".stripMargin
      SlackUtil.sendMessage(message)
    }

  }
}

object PersistentAPIErrorReporter {
  def default[T](clazz: Class[T]): PersistentAPIErrorReporter =
    default(clazz.getCanonicalName.stripSuffix("$"))
  def default(resourceName: String): PersistentAPIErrorReporter =
    new DefaultPersistentAPIErrorReporter(resourceName)
}
