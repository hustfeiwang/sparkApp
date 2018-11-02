package sqlApp

import org.apache.spark.sql.SparkSession

object parquetWrite {
  def main(args:Array[String]): Unit ={
    require(args.length==3,"UASGE: <master> <jsonFile> <parquet file>")
    val spark = SparkSession
      .builder()
      .appName("saveAsTable").master(args(0))
//      .enableHiveSupport()
      .getOrCreate()

    val df = spark.read.json(args(1))
    df.show()



//    df.write.format(args(1)).mode("overwrite").saveAsTable(args(2))
    df.repartition(5).write.mode("overwrite").parquet(args(2))
  }


}
