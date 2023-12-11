package io.klaytn.dsl

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import io.klaytn.utils.config.Cfg

import java.sql.Connection
import javax.sql.DataSource

package object db {
  private val dataSources =
    scala.collection.mutable.Map[String, HikariDataSource]()

  def withDB[R](dbName: String)(block: Connection => R): R = {
    withDB(getDataSource(dbName)) {
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

  def getDataSource(dbName: String): DataSource = {
    Class.forName("com.mysql.jdbc.Driver")

    this.synchronized {
      if (!dataSources.contains(dbName)) {
        val url = Cfg.getString(s"spark.app.mysql.url.$dbName")
        val urlProperty = Cfg.getString("spark.app.mysql.urlProperty")

        val dbConf = new HikariConfig()
        dbConf.setJdbcUrl(s"$url?$urlProperty")
        dbConf.setUsername(Cfg.getString("spark.app.mysql.username"))
        dbConf.setPassword(Cfg.getString("spark.app.mysql.password"))
        dbConf.addDataSourceProperty(
          "cachePrepStmts",
          Cfg.getString("spark.app.mysql.cachePrepStmts"))
        dbConf.addDataSourceProperty(
          "prepStmtCacheSize",
          Cfg.getString("spark.app.mysql.prepStmtCacheSize"))
        dbConf.addDataSourceProperty(
          "prepStmtCacheSqlLimit",
          Cfg.getString("spark.app.mysql.prepStmtCacheSqlLimit"))
        dbConf.addDataSourceProperty("allowLoadLocalInfile", "true")
        dbConf.setMinimumIdle(Cfg.getInt("spark.app.mysql.minimumIdle"))
        dbConf.setMaximumPoolSize(Cfg.getInt("spark.app.mysql.maximumPoolSize"))
        dbConf.setConnectionTimeout(
          Cfg.getInt("spark.app.mysql.connectionTimeout"))
        dbConf.addDataSourceProperty("useServerPrepStmts", "true")
        dbConf.setAutoCommit(false)

        dbConf.setPoolName(dbName)

        dataSources.put(dbName, new HikariDataSource(dbConf))
      }

      dataSources(dbName)
    }
  }
}
