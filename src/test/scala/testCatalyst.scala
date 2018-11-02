import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object testCatalyst {
  def main(args:Array[String]): Unit ={
    val spark = SparkSession.builder().master("local")
      .config(new SparkConf())
      .getOrCreate()

    val ta=spark.read.format("json").load("data/kv.json").registerTempTable("ta")
    val tb=spark.read.format("json").load("data/kv.json").registerTempTable("tb")

    val sql=spark.sql("select sum(v)" +
      "from (" +
      "select ta.key," +
      "1+2+ta.value As v\t"  +
      "from ta join tb\t" +
      "where\t" +
      "ta.key=tb.key\t" +
      "and tb.value>90)" +
      " tmp  ")
    sql.show()
    sql.explain(true)



  }

}
