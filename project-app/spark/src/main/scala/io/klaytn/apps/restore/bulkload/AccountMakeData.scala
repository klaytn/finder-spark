package io.klaytn.apps.restore.bulkload

import io.klaytn.model.Block
import io.klaytn.model.finder.Contract
import io.klaytn.utils.JsonUtil
import io.klaytn.utils.spark.SparkHelper

import scala.collection.mutable

/*
--driver-memory 10g
--num-executors 8
--executor-cores 4
--executor-memory 3g
--conf spark.app.phase=prod-cypress-modify-me
--class io.klaytn.apps.restore.bulkload.AccountMakeData
 */
object AccountMakeData extends SparkHelper with BulkLoadHelper {
  import AccountMakeDataDeps._

  def step1(start: Int, end: Int): Unit = {
    start to end foreach { bnp =>
      val rdd = sc
        .textFile(s"s3a://${kafkaLogDirPrefix()}/topic=block/bnp=$bnp/*.gz")
        .flatMap(Block.parse)
      if (!rdd.isEmpty()) {
        accountService
          .processWithoutSave(rdd, 256)
          .map {
            case (address, typ, txCount) =>
              s"$address\t$typ\t$txCount"
          }
          .repartition(32)
          .saveAsTextFile(s"s3a://${outputDirPrefix()}/AccountBatch/$bnp")
      }
    }
  }

  def step2(start: Int, end: Int): Unit = {
    val paths = start to end map (bnp =>
      s"s3a://${outputDirPrefix()}/AccountBatch/$bnp/part*")
    val rdd = sc.textFile(paths.mkString(","))
    rdd
      .repartition(256)
      .mapPartitions { lines =>
        val m = mutable.Map.empty[String, (String, Int)]
        lines.foreach { line =>
          val s = line.split("\t")
          val (address, typ, txCount) = (s(0), s(1), s(2).toInt)

          m.get(address) match {
            case Some(d) =>
              if (m(address)._1 == "EOA") {
                m(address) = (typ, d._2 + txCount)
              } else {
                m(address) = (d._1, d._2 + txCount)
              }
            case _ =>
              m(address) = (typ, txCount)
          }
        }
        m.iterator
      }
      .groupByKey(256)
      .map {
        case (address, _data) =>
          val data = _data.toSeq

          val contractFrom = data.filter(_._1 != "EOA")
          val txCount = data.map(_._2).sum
          if (contractFrom.nonEmpty) {
            (address, contractFrom.head._1, txCount)
          } else {
            (address, "EOA", txCount)
          }
      }
      .repartition(256)
      .map(x => s"${x._1}\t${x._2}\t${x._3}")
      .saveAsTextFile(
        s"s3a://${outputDirPrefix()}/AccountBatch/summary_${start}_$end/total")
  }

  def step3(start: Int, end: Int): Unit = {
    val rdd = sc
      .textFile(
        s"s3a://${outputDirPrefix()}/AccountBatch/summary_${start}_$end/total/part*")
      .map { line =>
        val s = line.split("\t")
        (s(0), s(1), s(2).toInt)
      }

    rdd
      .filter(_._2 == "EOA")
      .map(x => s"${x._1}\t${x._2}\t${x._3}")
      .repartition(256)
      .saveAsTextFile(
        s"s3a://${outputDirPrefix()}/AccountBatch/summary_${start}_$end/total_eoa")
    rdd
      .filter(_._2 != "EOA")
      .map(x => s"${x._1}\t${x._2}\t${x._3}")
      .repartition(256)
      .saveAsTextFile(
        s"s3a://${outputDirPrefix()}/AccountBatch/summary_${start}_$end/total_sca")
  }

  def step4(start: Int, end: Int): Unit = {
    sc.textFile(
        s"s3a://${outputDirPrefix()}/AccountBatch/summary_${start}_$end/total_eoa/part*")
      .map { line =>
        val s = line.split("\t")
        val (address, _, _) = (s(0), s(1), s(2).toInt)

        s"$address\t0\t0\t127"
      }
      .repartition(1024)
      .saveAsTextFile(
        s"s3a://${outputDirPrefix()}/AccountBatch/summary_${start}_$end/total_eoa_db")
  }

  def step5(start: Int, end: Int): Unit = {
    sc.textFile(
        s"s3a://${outputDirPrefix()}/AccountBatch/summary_${start}_$end/total_sca/part*")
      .map { line =>
        val s = line.split("\t")
        val (address, inputData, _) = (s(0), s(1), s(2).toInt)

        val m = JsonUtil.fromJson[Map[String, String]](inputData)
        val contract = if (m.get("tx_error") == "0") {
          caverContractService.getContract(address, m.get("input"))
        } else {
          Contract(address,
                   io.klaytn.model.finder.ContractType.Custom,
                   None,
                   None,
                   None,
                   None)
        }

        val name = contract.name.getOrElse("NIL")
        val symbol = contract.symbol.getOrElse("NIL")
        val decimal = contract.decimal.getOrElse(0)
        val totalSupply =
          BigDecimal(contract.totalSupply.getOrElse(BigInt(0))).bigDecimal

        s"$address\t${contract.contractType.id}\t$name\t$symbol\t$decimal\t$totalSupply\t${m
          .get("from")}\t${m.get("txHash")}\t${m.get("tx_error")}"
      }
      .saveAsTextFile(
        s"s3a://${outputDirPrefix()}/AccountBatch/summary_${start}_$end/total_sca_raw_data")
  }

  def step6(start: Int, end: Int): Unit = {
    val rdd =
      sc.textFile(
        s"s3a://${outputDirPrefix()}/AccountBatch/summary_${start}_$end/total_sca_raw_data/part*")
    rdd
      .flatMap { line =>
        val r =
          "([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^$]*)".r

        r.findAllIn(line).matchData map { m =>
          val (address,
               contractType,
               name,
               symbol,
               decimal,
               totalSupply,
               from,
               txHash,
               txError) =
            (m.group(1),
             m.group(2),
             m.group(3),
             m.group(4),
             m.group(5),
             m.group(6),
             m.group(7),
             m.group(8),
             m.group(9))
          s"$address\t$contractType\t$name\t$symbol\t$decimal\t$totalSupply\t$txError"
        }
      }
      .repartition(32)
      .saveAsTextFile(
        s"s3a://${outputDirPrefix()}/AccountBatch/summary_${start}_$end/total_sca_db_contracts")

    rdd
      .flatMap { line =>
        val r =
          "([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^$]*)".r

        r.findAllIn(line).matchData map { m =>
          val (address,
               contractType,
               name,
               symbol,
               decimal,
               totalSupply,
               from,
               txHash,
               txError) =
            (m.group(1),
             m.group(2),
             m.group(3),
             m.group(4),
             m.group(5),
             m.group(6),
             m.group(7),
             m.group(8),
             m.group(9))
          s"$address\t1\t0\t$contractType\t$from\t$txHash"
        }
      }
      .repartition(32)
      .saveAsTextFile(
        s"s3a://${outputDirPrefix()}/AccountBatch/summary_${start}_$end/total_sca_db_accounts")
  }

  override def run(args: Array[String]): Unit = {
    sendSlackMessage()

    // Categorize EOA SCA and save to one file
    step1(start, end)

    // aggregate all the data categorized by 100,000 in step1
    step2(start, end)

    // Categorize the data aggregated in step2 into SCA and EOA and save them respectively.
    step3(start, end)

    // Save EOA data in a format that can be loaded into S3 load data from S3
    step4(start, end)

    // Get SCA data contract type and save data
    step5(start, end)

    // Save the data created in step 6 to the accounts and contracts tables in a format that can be loaded as load data from s3
    step6(start, end)
  }
}
