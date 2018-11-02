package jira

import org.apache.spark.sql.SparkSession

object BD939 {
  def main(args: Array[String]): Unit = {
    require(args.length==6,"UASGE:  <inputTable> <partitionNum> <format> <path> <compression> <saveTableName")
    val spark = SparkSession
      .builder()
      .appName("BDSERVER-939 create orc table")
      .enableHiveSupport()
      .getOrCreate()

    val df = spark.table(args(0))

    df.repartition(args(1).toInt)
      .write.format(args(2))
      .option("path",args(3))
      .option("compression",args(4))
      .saveAsTable(args(5))

  }

}
