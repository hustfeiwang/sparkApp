package jira

import org.apache.spark.sql.SparkSession

object BD939NoPath {
  def main(args: Array[String]): Unit = {
    require(args.length==5,"UASGE:  <inputTable> <partitionNum> <format>  <compression> <saveTableName")
    val spark = SparkSession
      .builder()
      .appName("BDSERVER-939 create orc table")
      .enableHiveSupport()
      .getOrCreate()

    val df = spark.table(args(0))

    df.repartition(args(1).toInt)
      .write.format(args(2))
      .option("compression",args(3))
      .saveAsTable(args(4))

  }

}
