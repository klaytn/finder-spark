package io.klaytn.tools.dsl

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

import java.sql.Connection
import javax.sql.DataSource

package object db {
  private val dataSources =
    scala.collection.mutable.Map[String, HikariDataSource]()

  def getDatasource(dbName: String): DataSource = {
    Class.forName("com.mysql.jdbc.Driver")

    val username = ""
    val password = ""

    if (!dataSources.contains(dbName)) {
      val dbConf = new HikariConfig()
      val url = dbName match {
        case "baobabr" =>
          "jdbc:mysql://ENV_MYSQL_HOST:3306/finder"
        case "baobab" =>
          "jdbc:mysql://ENV_MYSQL_HOST:3306/finder"

        case "finder0101r" =>
          "jdbc:mysql://ENV_MYSQL_HOST:3306/finder"
        case "finder0101" =>
          "jdbc:mysql://ENV_MYSQL_HOST:3306/finder"
        case "finder0201r" =>
          "jdbc:mysql://ENV_MYSQL_HOST306/finder01"
        case "finder0202r" =>
          "jdbc:mysql://ENV_MYSQL_HOST306/finder02"
        case "finder0203r" =>
          "jdbc:mysql://ENV_MYSQL_HOST306/finder03"
        case "finder0204r" =>
          "jdbc:mysql://ENV_MYSQL_HOST306/finder04"
        case "finder0205r" =>
          "jdbc:mysql://ENV_MYSQL_HOST306/finder05"
        case "finder0206r" =>
          "jdbc:mysql://ENV_MYSQL_HOST306/finder06"
        case "finder0207r" =>
          "jdbc:mysql://ENV_MYSQL_HOST306/finder07"
        case "finder0208r" =>
          "jdbc:mysql://ENV_MYSQL_HOST306/finder08"
        case "finder0209r" =>
          "jdbc:mysql://ENV_MYSQL_HOST306/finder09"
        case "finder0210r" =>
          "jdbc:mysql://ENV_MYSQL_HOST306/finder10"
        case "finder03r" =>
          "jdbc:mysql://ENV_MYSQL_HOST:3306/finder"
        case "finder03" =>
          "jdbc:mysql://ENV_MYSQL_HOST:3306/finder"
      }

      dbConf.setJdbcUrl(
        s"$url?serverTimezone=UTC&rewriteBatchedStatements=true&useSSL=false")
      dbConf.setUsername(username)
      dbConf.setPassword(password)
      dbConf.addDataSourceProperty("useServerPrepStmts", "true")
      dbConf.setAutoCommit(false)

      dbConf.setPoolName(dbName)

      dataSources.put(dbName, new HikariDataSource(dbConf))
    }
    dataSources(dbName)
  }

  def withDB[R](dbName: String)(block: Connection => R): R = {
    withDB(getDatasource(dbName)) {
      block
    }
  }

  def withDB[R](ds: DataSource)(block: Connection => R): R = {
    val connection = ds.getConnection()

    try {
      block(connection)
    } finally {
      connection.commit()
      connection.close()
    }
  }
}
