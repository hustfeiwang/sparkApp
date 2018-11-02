package sqlApp

import org.apache.spark.sql.SparkSession

object readTable {
  def main(args:Array[String]): Unit ={
    require(args.length==3,"UASGE:  <format> <tableName> <path> ")
    val spark = SparkSession
      .builder()
      .appName("readTable").enableHiveSupport()
      .getOrCreate()


    val df =spark.read.format(args(0)).option("path",args(2)).table(args(1))
    df.show()




  }

}
