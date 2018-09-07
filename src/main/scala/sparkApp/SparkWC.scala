package sparkApp

import org.apache.spark.{SparkConf, SparkContext}

object SparkWC {
  def main(args:Array[String]): Unit ={
    require(args.length>0,"usage WordCount <master url>")
    val sc = new SparkContext(new SparkConf().setMaster(args(0)).setAppName("wordCount"))
    sc.parallelize((1 to 100),2).map((_,1))
      .reduceByKey(_+_)
      .foreach(println)
  }

}
