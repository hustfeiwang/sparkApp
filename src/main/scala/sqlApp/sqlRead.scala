package sqlApp

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
object sqlRead {
  def main(args:Array[String]): Unit ={

    require(args.length==1,"the name to read table")
    val spark = SparkSession
      .builder()
      .appName("sqlRead").enableHiveSupport()
      .getOrCreate()
    import  spark.implicits._

    val tb1=spark.read.format("json").table(args(0))
    tb1.show()
    spark.sql("").rdd.collect()

  }

}