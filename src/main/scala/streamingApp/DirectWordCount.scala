package streamingApp
import kafka.serializer.StringDecoder
import org.apache.spark._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka._

object DirectWordCount {

  def main(args:Array[String]): Unit ={

    require(args.length>2,
      s"""
         |Parameter Usage: KafkaWordCount <brokers> <topics>
         |<brokers> is a list of one or more kafka brokers splited by ","
         |<topics> is a list of one or more kafka topics splited by ","
       """.stripMargin)

    val Array(brokers, topics)=args

    val sparkConf = new SparkConf().setAppName("kafkaWordCount")
    val ssc = new StreamingContext(sparkConf,Seconds(2))

    val topicSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams,topicSet)

    val lines = messages.map(_._2)
    val words = lines.flatMap(_.split(""))
    val wordCount= words.map((_,1)).reduceByKey(_+_)
    wordCount.print()
    ssc.start()
    ssc.awaitTerminationOrTimeout(20000)

  }


}
