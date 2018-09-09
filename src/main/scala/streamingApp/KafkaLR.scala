package streamingApp

import java.util.{HashMap, Random}


import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import breeze.linalg.Vector
import breeze.linalg.DenseVector
/**
  * Consumes messages from one or more topics in Kafka and does wordcount.
  * Usage: KafkaWordCount <bootstraps> <group> <topics>
  *   <bootstraps> is a list of one or more bootStrap servers
  *   <group> is the name of kafka consumer group
  *   <topics> is a list of one or more kafka topics to consume from
  *
  * Example:
  *    `$ bin/run-example \
  *      org.apache.spark.examples.streaming.KafkaLR bootStrap1,bootStrap2 \
  *      my-consumer-group topic1,topic2 `
  */
object KafkaLR {
  case class DataPoint(x: Vector[Double], y: Double)
  val rand = new Random(42)

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: KafkaWordCount <bootstraps> <group> <topics> ")
      System.exit(1)
    }

    val Array(bootStraps, group, topics) = args
    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> bootStraps,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topicArray = topics.split(",")
    val lines = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String] (topicArray, kafkaParams)
    ).map(_.value()).flatMap(s=> s.split("\n"))

    val points = lines.filter(s=> s.split("\\s+").length==11).map{line =>
      val parts = line.split(" ")
      val y = parts(0).toDouble
      val data = new Array[Double](parts.length-1)
      for(i <- 1 until parts.length){
        data(i-1) = parts(i).toDouble
      }
      val x = DenseVector(data)
      DataPoint(x,y)
    }
    points.print()
    val iterations = 1
    val length = 10
    var w = DenseVector.fill(length){2 * rand.nextDouble - 1}

    for( i<- 1 to iterations){
      val gradient = points.map { p =>
        p.x * (1 / (1 + Math.exp(-p.y * (w.dot(p.x)))) - 1) * p.y
      }.reduce(_ + _)
//      w -= gradient
    }
    println("Final w: " + w)
    ssc.start()
    ssc.awaitTermination()
  }
}

// Produces some random words between 1 and 100.
object KafkaLRProducer {

  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KafkaLRProducer <metadataBrokerList> <topic> " +
        "<messagesPerSec> <wordsPerMessage>")
      System.exit(1)
    }

    val Array(brokers, topic, messagesPerSec, wordsPerMessage) = args

    // Zookeeper connection properties
    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    // Send some messages
    while(true) {
      (1 to messagesPerSec.toInt).foreach { messageNum =>
        val str = (1 to wordsPerMessage.toInt).map(x => {
          var r = "1"
          for(i <- 1 to 10){
            r += "\t"+ scala.util.Random.nextInt(10000).toString
          }
          r
        }).mkString("\n")

        val message = new ProducerRecord[String, String](topic, null, str)
        producer.send(message)
      }

      Thread.sleep(1000)
    }
  }

}