package io.klaytn.repository

import io.klaytn.utils.SlackUtil
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.exception.ExceptionUtils

import java.sql.PreparedStatement
import java.text.SimpleDateFormat
import java.util.TimeZone
import scala.collection.mutable.ArrayBuffer

abstract class AbstractRepository extends Serializable {
  private val name = getClass.getSimpleName

  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
  sdf.setTimeZone(TimeZone.getTimeZone("UTC"))

  def execute(preparedStatement: PreparedStatement): Array[Int] = {
    execute(preparedStatement, ArrayBuffer.empty)
  }

  def execute(preparedStatement: PreparedStatement,
              logs: ArrayBuffer[String]): Array[Int] = {
    try {
      val result = preparedStatement.executeBatch()
      preparedStatement.clearBatch()
      result
    } catch {
      case e: Throwable =>
        SlackUtil.sendMessage(s"""$name Error
                                 |${logs.mkString("\n")}
                                 |${e.getLocalizedMessage}
                                 |stacktrace: ${StringUtils.abbreviate(
                                   ExceptionUtils.getMessage(e),
                                   500)}
                                 |""".stripMargin)
        throw e
    }
  }
}
