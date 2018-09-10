package streamingApp
import java.util.HashMap

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

/**
  * Consumes messages from one or more topics in Kafka and does wordcount.
  * Usage: KafkaCC <bootstraps> <group> <topics> <timeOut>
  *   <bootstraps> is a list of one or more bootStrap servers
  *   <group> is the name of kafka consumer group
  *   <topics> is a list of one or more kafka topics to consume from
  *   <timeOut> is the time to terminated the streaming app
  *
  * Example:
  *    `$ bin/run-example \
  *      org.apache.spark.examples.streaming.KafkaCC bootStrap1,bootStrap2 \
  *      my-consumer-group topic1,topic2 `
  */
object KafkaCC {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCount <bootstraps> <group> <topics> <timeOut> ")
      System.exit(1)
    }

    val Array(bootStraps, group, topics, timeOut) = args
    val sparkConf = new SparkConf().setAppName("KafkaCC")
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

    val edges = lines.filter(_.trim!="").map(_.trim).map{ s=>
      val parts = s.split("\\s+")
      (parts(0).toInt, parts(1).toInt)
    }

    val g = edges.groupByKey()
    var messages = g.map(eMsg => {
      (eMsg._1, eMsg._1)
    })
    //此处只迭代 一次
    for ( i <- 1 to 1){
      val newVertices = g.join(messages).map(_._2).flatMap( value => {
        value._1.map(vtx => (vtx, math.min(vtx, value._2)))
      })
      messages = newVertices.reduceByKey((v1,v2)=>math.min(v1,v2))
    }
    messages.print()

    ssc.start()
    ssc.awaitTerminationOrTimeout(timeOut.toInt * 1000)
    ssc.stop()
  }
}

// Produces some random words between 1 and 100.
object KafkaCCProducer {

  def main(args: Array[String]) {
    if (args.length < 5) {
      System.err.println("Usage: KafkaCCProducer <metadataBrokerList> <topic> " +
        "<messagesPerSec> <wordsPerMessage> <lifeTime>")
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
          for(i <- 1 to 2){
            r += "\t" + scala.util.Random.nextInt(10000).toString
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