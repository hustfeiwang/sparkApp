package sqlApp

import org.apache.spark.sql.SparkSession

object analyzeTableColumns {
  def main(args:Array[String])={
    require(args.length>0," dataBaseName")
    val dataBase=args(0)
    val spark= SparkSession.builder()
      .appName("analyzeTable"+dataBase)
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("use "+dataBase).show()
    val tables= spark.sql("show tables").rdd.collect()

    for(tableArr<- tables){
      val table=tableArr(1)
      val columns= spark.sql("show columns from "+table).rdd.collect()

      val colStr=columns.map(arr=>arr(0)).mkString(",")

      val sqlString=" analyze table "+table+"\tCOMPUTE STATISTICS FOR COLUMNS\t"+colStr
      println(sqlString)
      spark.sql(sqlString).show()
    }
    spark.stop()

  }

}

