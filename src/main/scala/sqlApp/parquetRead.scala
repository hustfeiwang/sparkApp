package sqlApp

import org.apache.spark.sql.SparkSession

object parquetRead {
  def main(args:Array[String]): Unit = {
    require(args.length == 2, "UASGE: <master>  <parquet file>")
    val spark = SparkSession
      .builder()
      .appName("saveAsTable").master(args(0))
      //      .enableHiveSupport()
      .getOrCreate()

    spark.read.parquet(args(1)).registerTempTable("ta")
    spark.read.parquet(args(1)).registerTempTable("tb")

    spark.sql("select sum(v) from ( select ta.age, 1+2+ta.age as v from ta join tb where ta.name=tb.name And ta.age>17 And " +
      "tb.age<30) temp ").explain(true)
  }


}
