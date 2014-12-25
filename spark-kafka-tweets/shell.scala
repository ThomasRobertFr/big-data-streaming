import org.apache.spark.streaming.{Seconds, StreamingContext}
import consumer.kafka.client.KafkaReceiver

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
val kafkaStreams = (1 to partitions).map { i=>
  ssc.receiverStream(new KafkaReceiver(props, i))
}

//union everything into one stream
val tmp_stream = ssc.union(kafkaStreams)

tmp_stream.foreachRDD(rdd => println("\n\nNumber of records in this batch : " + rdd.count()))

//Lets convert the Array[Byte] to String
val stream = tmp_stream.map(x => { val s = new String(x.getPayload); s })

stream.print()

ssc.start()

