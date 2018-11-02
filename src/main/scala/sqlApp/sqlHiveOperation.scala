package sqlApp

import java.sql.DriverManager
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object sqlHiveOperation {
  def main(args:Array[String]): Unit ={

    require(args.length == 2, "sqlHiveOperation Usage: <URL> <Table_Name>")

    val Array(url,table_name) = args

    val spark = SparkSession
      .builder()
      .appName("sqlHiveOperation").enableHiveSupport()
      .getOrCreate()
    import  spark.implicits._

    val readConnProperties1 = new Properties()
    readConnProperties1.put("driver", "org.apache.hadoop.hive.jdbc.HiveDriver")
    val jdbcDf = spark.read.jdbc(url,table_name,readConnProperties1)

    jdbcDf.show()








  }

}
