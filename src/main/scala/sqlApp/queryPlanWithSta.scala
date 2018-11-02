package sqlApp

import java.io._
import org.apache.spark.sql.SparkSession
import  java.util.{ArrayList => JList}

object queryPlanWithSta {
  def main(args:Array[String]): Unit ={
    require(args.length>0," sqlFile")
    val sqlFileName=args(0)
    val spark= SparkSession.builder()
      .appName("analyzeTable"+sqlFileName)
      .enableHiveSupport()
      .getOrCreate()

    val sql= new JList[String]()
    try {
      val fr= new DataInputStream(new FileInputStream(sqlFileName))
      var line =fr.readLine()
      while (line!=null){

        val fi = line.indexOf("#")
        if(fi == -1){
          sql.add(line)
        }else{
          sql.add(line.substring(0,fi))
        }

        line = fr.readLine()
      }
      fr.close()
    }catch {
      case e:Exception=>
        e.printStackTrace()
    }

    val sqlStringArr= sql.toArray().mkString("\t").split(";")
    for( sqlStr<- sqlStringArr){
      println(spark.sql(sqlStr).queryExecution.stringWithStats)
    }


  }

}
