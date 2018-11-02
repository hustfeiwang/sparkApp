import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object testJoin {
  def main(args:Array[String]): Unit ={
    val spark = SparkSession.builder().master("local")
      .config(new SparkConf())
      .getOrCreate()

    val begin =System.currentTimeMillis()
    val tableA = spark.range(20000000).as('a)
    val tableB = spark.range(10000000).as('b)
//    val result= tableA.join(tableB,Seq("id"))
//      .groupBy()
//      .count()
//    result.show()

   tableA.join(tableB,Seq("id")).select("id").explain()

//    val end=System.currentTimeMillis()
//    val cost =end - begin
//    println("time is " +cost)
//    result.explain()


  }

}
