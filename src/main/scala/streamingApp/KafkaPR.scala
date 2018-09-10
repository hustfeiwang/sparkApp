package streamingApp

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
import java.util.HashMap

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
/**
  * Consumes messages from one or more topics in Kafka and does wordcount.
  * Usage: KafkaPR <bootstraps> <group> <topics> <timeOut>
  *   <bootstraps> is a list of one or more bootStrap servers
  *   <group> is the name of kafka consumer group
  *   <topics> is a list of one or more kafka topics to consume from
  *   <timeOut> is the time to terminated the streaming app
  *
  * Example:
  *    `$ bin/run-example \
  *      org.apache.spark.examples.streaming.KafkaPR bootStrap1,bootStrap2 \
  *      my-consumer-group topic1,topic2 timeOut `
  */
object KafkaPR {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCount <bootstraps> <group> <topics> <timeOut>")
      System.exit(1)
    }

    val Array(bootStraps, group, topics, timeOut) = args
    val sparkConf = new SparkConf().setAppName("KafkaPR")
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


    val links = lines.filter(_.trim!="").map(_.trim).map{ s=>
      val parts = s.split("\\s+")
      (parts(0).toInt, parts(1).toInt)
    }.groupByKey()
    var ranks = links.mapValues(v => 1.0)
    for( i<- 1 to 1){
      val contribs = links.join(ranks).map(_._2).flatMap { case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }
    ranks.print()

    ssc.start()
    ssc.awaitTerminationOrTimeout(timeOut.toInt*1000)
    ssc.stop()
  }
}

// Produces some random words between 1 and 100.
object KafkaPRProducer {

  def main(args: Array[String]) {
    if (args.length < 5) {
      System.err.println("Usage: KafkaPRProducer <metadataBrokerList> <topic> " +
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
    while(lastTime >0 ) {
      (1 to messagesPerSec.toInt).foreach { messageNum =>
        val str = (1 to wordsPerMessage.toInt).map(x => {
          var r = ""
          for(i <- 1 to 2){
            r += "\t"+ scala.util.Random.nextInt(10000).toString
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