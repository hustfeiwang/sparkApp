package sqlApp

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
object sqlWrite {
  def main(args:Array[String]): Unit ={

    val spark = SparkSession
      .builder()
      .appName("sqlWriter").master("local")
      .getOrCreate()
    import  spark.implicits._

    val readConnProperties1 = new Properties()
    readConnProperties1.put("driver", "com.mysql.jdbc.Driver")
    readConnProperties1.put("user", "root")
    readConnProperties1.put("password", "19920531")
    readConnProperties1.put("fetchsize", "1")

    val jdbcDF1 = spark.read.jdbc(
      "jdbc:mysql://localhost:3306",
      "insight",
      readConnProperties1)
    spark.sql("show tables").show()


    jdbcDF1.write.format("json").mode("overwrite")saveAsTable("test123")

    spark.sql("show tables").show()

    spark.read.format("json").table("test123").show()


  }

}

