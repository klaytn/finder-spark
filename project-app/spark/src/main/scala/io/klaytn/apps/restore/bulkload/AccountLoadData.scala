package io.klaytn.apps.restore.bulkload

import io.klaytn.dsl.db.withDB
import io.klaytn.repository.{AccountRepository, ContractRepository}
import io.klaytn.utils.SlackUtil
import io.klaytn.utils.spark.SparkHelper

/*
--driver-memory 10g
--num-executors 2
--executor-cores 4
--executor-memory 3g
--conf spark.app.phase=prod-cypress-modify-me
--class io.klaytn.apps.restore.bulkload.AccountLoadData
 */
object AccountLoadData extends SparkHelper with BulkLoadHelper {
  def step1(start: Int, end: Int): Unit = {
    sc.parallelize(0 until 1024 map (x => f"part-$x%05d"))
      .repartition(1)
      .foreach { filename =>
        val s3Path =
          s"${outputDirPrefix()}/AccountBatch/summary_${start}_$end/total_eoa_db/$filename"
        //
        val sql =
          s"""LOAD DATA FROM S3 's3://$s3Path'
           |IGNORE INTO TABLE `accounts`
           |FIELDS TERMINATED BY '\t'
           |LINES TERMINATED BY '\n'
           |(`address`,`account_type`,`total_transaction_count`,`contract_type`)
           |""".stripMargin

        withDB(AccountRepository.AccountDB) { c =>
          val pstmt = c.prepareStatement(sql)
          try {
            pstmt.execute()
          } catch {
            case e: Throwable =>
              SlackUtil.sendMessage(s"${e.getLocalizedMessage}: $s3Path")
              e.printStackTrace()
              throw e
          } finally {
            pstmt.close()
          }
        }
      }
  }

  def step2(start: Int, end: Int): Unit = {
    sc.parallelize(0 until 32 map (x => f"part-$x%05d"))
      .repartition(1)
      .foreach { filename =>
        val s3Path =
          s"${outputDirPrefix()}/AccountBatch/summary_${start}_$end/total_sca_db_accounts/$filename"
        val sql =
          s"""LOAD DATA FROM S3 's3://$s3Path'
           |IGNORE INTO TABLE `accounts`
           |FIELDS TERMINATED BY '\t'
           |LINES TERMINATED BY '\n'
           |(`address`,`account_type`,`total_transaction_count`,`contract_type`,`contract_creator_address`,`contract_creator_transaction_hash`)
           |""".stripMargin

        withDB(AccountRepository.AccountDB) { c =>
          val pstmt = c.prepareStatement(sql)
          try {
            pstmt.execute()
          } catch {
            case e: Throwable =>
              SlackUtil.sendMessage(s"${e.getLocalizedMessage}: $s3Path")
              e.printStackTrace()
              throw e
          } finally {
            pstmt.close()
          }
        }
      }

    sc.parallelize(0 until 32 map (x => f"part-$x%05d"))
      .repartition(1)
      .foreach { filename =>
        val s3Path =
          s"${outputDirPrefix()}/AccountBatch/summary_${start}_$end/total_sca_db_contracts/$filename"
        val sql =
          s"""LOAD DATA FROM S3 's3://$s3Path'
           |IGNORE INTO TABLE `contracts`
           |FIELDS TERMINATED BY '\t'
           |LINES TERMINATED BY '\n'
           |(`contract_address`,`contract_type`,@name,@symbol,`decimal`,`total_supply`,`tx_error`)
           |SET
           |`name` = NULLIF(@name, 'NIL'),
           |`symbol` = NULLIF(@symbol, 'NIL')
           |""".stripMargin

        withDB(ContractRepository.ContractDB) { c =>
          val pstmt = c.prepareStatement(sql)
          try {
            pstmt.execute()
          } catch {
            case e: Throwable =>
              SlackUtil.sendMessage(s"${e.getLocalizedMessage}: $s3Path")
              e.printStackTrace()
              throw e
          } finally {
            pstmt.close()
          }

        }
      }
  }

  override def run(args: Array[String]): Unit = {
    sendSlackMessage()

    // load eoa data load data from s3
    step1(start, end)

    // Load SCA data Load data from S3 (accounts, contracts)
    step2(start, end)
  }
}
