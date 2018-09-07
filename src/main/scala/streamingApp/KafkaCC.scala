package streamingApp
import java.util.HashMap

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

/**
  * Consumes messages from one or more topics in Kafka and does wordcount.
  * Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>
  *   <zkQuorum> is a list of one or more zookeeper servers that make quorum
  *   <group> is the name of kafka consumer group
  *   <topics> is a list of one or more kafka topics to consume from
  *   <numThreads> is the number of threads the kafka consumer should use
  *
  * Example:
  *    `$ bin/run-example \
  *      org.apache.spark.examples.streaming.KafkaWordCount zoo01,zoo02,zoo03 \
  *      my-consumer-group topic1,topic2 1`
  */
object KafkaCC {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }


    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("KafkaCC")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    // 取到数据的value部分，是个字符串，里面是两个整数
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
      .flatMap( s=> s.split("\n"))

    val edges = lines.filter(s=> s.split("\\s+").length==2).map{ s=>
      val parts = s.split("\\s+")
      (parts(0).toInt, parts(1).toInt)
    }

    val g = edges.groupByKey()
//    g.persist(StorageLevel.MEMORY_AND_DISK)
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
    ssc.awaitTermination()
  }
}

// Produces some random words between 1 and 100.
object KafkaCCProducer {

  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KafkaCCProducer <metadataBrokerList> <topic> " +
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
    }
  }

}