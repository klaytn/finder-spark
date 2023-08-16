package io.klaytn.test

object WordCount extends SparkSessionWrapper {
  def main(args: Array[String]): Unit = {
    import spark.implicits._

    val textFile =
      spark.read.textFile("project-app/spark/src/main/scala/words.txt")
    val wordCounts = textFile
      .flatMap(line => line.split(" "))
      .groupByKey(identity)
      .count()

    wordCounts.show()
  }
}
