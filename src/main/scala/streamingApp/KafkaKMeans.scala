package streamingApp

import java.util.HashMap

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import breeze.linalg.{DenseVector, Vector, squaredDistance}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

/**
  * Consumes messages from one or more topics in Kafka and does wordcount.
  * Usage: KafkaKMeans <bootstraps> <group> <topics> <timeOut>
  *   <bootstraps> is a list of one or more bootStrap servers
  *   <group> is the name of kafka consumer group
  *   <topics> is a list of one or more kafka topics to consume from
  *   <timeOut> is the time to terminated the streaming app
  *
  * Example:
  *    `$ bin/run-example \
  *      org.apache.spark.examples.streaming.KafkaKMeans bootStrap1,bootStrap2 \
  *      my-consumer-group topic1,topic2 `
  */
object KafkaKMeans {
  def parseVector(line: String): Vector[Double] = {
    DenseVector(line.split("\\s+").map(_.toDouble))
  }
  def closestPoint(p: Vector[Double], centers: Array[Vector[Double]]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity

    for (i <- 0 until centers.length) {
      val tempDist = squaredDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }

    bestIndex
  }


  def main(args: Array[String]) {

    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCount <bootstraps> <group> <topics> <timeOut> ")
      System.exit(1)
    }
    System.err.println(
      """WARN: This is a naive implementation of KMeans Clustering and is given as an example!
        |Please use the KMeans method found in org.apache.spark.mllib.clustering
        |for more conventional use.
      """.stripMargin)
    val Array(bootStraps, group, topics, timeOut) = args
    val sparkConf = new SparkConf().setAppName("KafkaKMeans")
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


    val data = lines.filter(_.trim!="").map(_.trim)map(parseVector _)
    val k =2
    val convergeDist = 0.3

    val kpoints = Array(parseVector("0.1\t0.1\t0.1"),parseVector("0.5\t0.5\t0.5"))
    var tempDist=1.0

    for(i <- 1 to 1 ){
      val closest = data.map(p => (closestPoint(p,kpoints),(p,1)))
      val pointStats = closest.reduceByKey{case ((x1, y1), (x2, y2)) => (x1 + x2, y1 + y2)}

      val newPoints = pointStats.map {pair =>
        (pair._1, pair._2._1 * (1.0 / pair._2._2))}
      newPoints.print()
    }
    ssc.start()
    ssc.awaitTerminationOrTimeout(timeOut.toInt * 1000)
    ssc.stop()


  }
}

// Produces some random words between 1 and 100.
object KafkaKMeansProducer {

  def main(args: Array[String]) {
    if (args.length < 5) {
      System.err.println("Usage: KafkaKMeansProducer <metadataBrokerList> <topic> " +
        "<messagesPerSec> <wordsPerMessage> <lifeTime> ")
      System.exit(1)
    }

    val Array(brokers, topic, messagesPerSec, wordsPerMessage, lifeTime) = args

    // Zookeeper connection properties
    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    var lastTime = lifeTime.toInt
    // Send some messages
    while(lastTime > 0) {
      (1 to messagesPerSec.toInt).foreach { messageNum =>
        val str = (1 to wordsPerMessage.toInt).map(x => {
          var r = ""
          for(i <- 1 to 3){
            r += "\t" + scala.util.Random.nextDouble().toString
          }
          r
        }).mkString("\n")

        val message = new ProducerRecord[String, String](topic, null, str)
        producer.send(message)
      }

      Thread.sleep(1000)
      lastTime -=1
    }
    producer.close()
  }

}