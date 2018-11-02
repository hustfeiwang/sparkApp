package sqlApp

import org.apache.spark.sql.SparkSession

object sparkSql {
  def main(args:Array[String]): Unit ={
    require(args.length>0,"the sql String")
    val spark = SparkSession
      .builder()
      .appName("sparkSql").enableHiveSupport()
      .getOrCreate()

    val sqls=args.mkString("\t").split(";")
    for(i<- 0 until sqls.length) {
      spark.sql(sqls(i)).show
    }
  }

}
