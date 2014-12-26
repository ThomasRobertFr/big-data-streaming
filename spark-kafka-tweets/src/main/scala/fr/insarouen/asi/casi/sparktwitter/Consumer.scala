package fr.insarouen.asi.casi.sparktwitter

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util.Date

import consumer.kafka.client.KafkaReceiver

object Consumer {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Simple Application")

    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(10))

    val topic = "TweetStatus"

    val zkhosts = "localhost"
    val zkports = "2181"
    val brokerPath = "/brokers"
    val partitions = 3

    val kafkaProperties: Map[String, String] = Map("zookeeper.hosts" -> zkhosts,
      "zookeeper.port" -> zkports,
      "zookeeper.broker.path" -> brokerPath ,
      "kafka.topic" -> topic,
      "zookeeper.consumer.connection" -> "localhost:2181",
      "zookeeper.consumer.path" -> "/spark-kafka",
      "kafka.consumer.id" -> "12345")

    val props = new java.util.Properties()
    kafkaProperties foreach { case (key,value) => props.put(key, value)}


    // Read data from all partitions
    val kafkaStreams = (1 to partitions).map { i =>
      ssc.receiverStream(new KafkaReceiver(props, i))
    }

    //union everything into one stream & convert the Array[Byte] to String
    val stream = ssc.union(kafkaStreams).map(x => { val s = new String(x.getPayload); s })


    // process every batch like this
    stream.foreachRDD(rdd => {
      println("\n\n=====================================================\nBatch @ " + new Date() + " (" + rdd.count() + " tweets)\n=====================================================\n")
      if (rdd.count() > 0) {
        rdd.take(5).foreach(println)
        if (rdd.count() > 0)
          println("...")
        else
          println("...\n\n--------------")
        println("Result : " + rdd.map(s => {
          val data = Map("like"->3,"enjoy"->5,"good"->5,"nice"->3,"great"->5,"love"->8,"appreciate"->3,"awesome"->8,":-)"->5,":)"->5,":D"->5,":o)"->5,":]"->5,":3"->5,":c)"->5,":>"->5,"=]"->5,"8)"->5,"=)"->5,":}"->5,":^)"->5,":-D"->7,"8-D"->7,"8D"->7,"x-D"->7,"xD"->7,"X-D"->7,"XD"->7,"=-D"->7,"=D"->7,"=-3"->7,"=3"->7,"B^D"->7,":-))"->7,":'-)"->7,":')"->3,":*"->3,":^*"->3,";-)"->3,";)"->3,"*-)"->3,"*)"->3,";-]"->3,";]"->3,";D"->3,";^)"->3,":-,"->3,">:P"->3,":-P"->3,":P"->3,"X-P"->3,"x-p"->3,"xp"->3,"XP"->3,":-p"->3,":p"->3,"=p"->3,":-Þ"->3,":Þ"->3,":þ"->3,":-þ"->3,":-b"->3,":b"->3,"d:"->3,"<3"->10,"hate"-> -8,"dislike"-> -5,"bad"-> -5,"awful"-> -7,"stupid"-> -5,">:["-> -5,":-("-> -5,":("-> -5,":-c"-> -5,":c"-> -5,":-<"-> -5,":っC"-> -5,":<"-> -5,":-["-> -5,":["-> -5,":{"-> -5,";("-> -5,":-||"-> -5,":@"-> -5,">:("-> -5,":'-("-> -8,":'("-> -8,"D:<"-> -5,"D:"-> -5,"D8"-> -5,"D;"-> -5,"D="-> -5,"DX"-> -5,"v.v"-> -5,"D-':"-> -5,">:\\"-> -2,">:/"-> -2,":-/"-> -2,":-."-> -2,":/"-> -2,":\\"-> -2,"=/"-> -2,"=\\"-> -2,":L"-> -2,"=L"-> -2,":S"-> -2,">.<"-> -2,":$"-> -2,"</3"-> -10)
          s.split(" ").map(s => data.getOrElse(s.toLowerCase, 0)).reduceLeft(_ + _)
        }).reduce(_ + _))
      }
    })

    ssc.start()
  }
}
