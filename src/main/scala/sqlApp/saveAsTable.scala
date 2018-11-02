package sqlApp

import org.apache.spark.sql.SparkSession

object saveAsTable {
  def main(args:Array[String]): Unit ={
    require(args.length==3,"UASGE: <jsonFile> <format> <tableName>")
    val spark = SparkSession
      .builder()
      .appName("saveAsTable")
      .enableHiveSupport()
      .getOrCreate()

    val df = spark.read.json(args(0))
    df.show()

    df.write.format(args(1)).mode("overwrite").saveAsTable(args(2))
  }

}
