package sqlApp

import org.apache.spark.sql.SparkSession

object readTableAndSql {

  def main(args:Array[String]): Unit ={
    require(args.length>=2,"UASGE:  <format> <tableName>")
    val spark = SparkSession
      .builder()
      .appName("saveAsTable")
      .enableHiveSupport()
      .getOrCreate()

    val sqls=args.mkString("\t").split(";")

    val format=sqls(0)
    val table_name=sqls(1)

    val df =spark.read.format(sqls(0)).table(sqls(1))
    df.show()

    for(i<-2 until sqls.length){
      spark.sql(sqls(i)).show()
    }

  }

}
