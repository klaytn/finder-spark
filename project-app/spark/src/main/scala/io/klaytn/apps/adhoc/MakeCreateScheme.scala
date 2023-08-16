package io.klaytn.apps.adhoc

import io.klaytn.utils.config.Cfg
import io.klaytn.utils.spark.{SparkHelper, UserConfig}

/** example
  *--packages org.apache.hadoop:hadoop-aws:3.3.1 \
  *--conf spark.app.s3a.access.key=... \
  *--conf spark.app.s3a.secret.key=... \
  *--conf spark.app.create.scheme.path.prefix="s3a://klaytn-prod-lake/klaytn" \
  *--conf spark.app.create.scheme.path.suffix="bnp=100/" \
  *--conf spark.app.create.scheme.path.partitionedBy="bnp int" \
  *--conf spark.app.create.scheme.table="transaction_receipts" \
  *--class io.klaytn.apps.MakeCreateScheme \
  */
object MakeCreateScheme extends SparkHelper {
  override def run(args: Array[String]): Unit = {
    sc.hadoopConfiguration.set("fs.s3a.access.key", UserConfig.s3AccessKey.get)
    sc.hadoopConfiguration.set("fs.s3a.secret.key", UserConfig.s3SecretKey.get)

    val prefix = Cfg.getString("spark.app.create.scheme.path.prefix")
    val suffix = Cfg.getString("spark.app.create.scheme.path.suffix")
    val partitionedBy =
      Cfg.getString("spark.app.create.scheme.path.partitionedBy")
    val tableName = Cfg.getString("spark.app.create.scheme.table")
    val path = s"$prefix/label=$tableName/$suffix"
    val df = spark.read.parquet(path)

    val cols = df.dtypes
      .map { col =>
        s"    ${col._1} ${col._2.replace("Type", "")}"
      }
      .mkString(",\n")

    val result =
      s"""CREATE EXTERNAL TABLE $tableName (
         |${cols})
         |PARTITIONED BY ( 
         |  $partitionedBy)
         |ROW FORMAT SERDE 
         |  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
         |STORED AS INPUTFORMAT
         |  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
         |OUTPUTFORMAT
         |  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'  
         |LOCATION
         |  '$prefix/label=$tableName/'
         |TBLPROPERTIES (
         |  'classification'='parquet')
         |""".stripMargin

    println(result)
  }
}
