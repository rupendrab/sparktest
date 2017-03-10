import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

val kafkaParams = Map[String, Object](
  "bootstrap.servers" -> "localhost:9092",
  "key.deserializer" -> classOf[StringDeserializer],
  "value.deserializer" -> classOf[StringDeserializer],
  "group.id" -> "use_a_separate_group_id_for_each_stream",
  "auto.offset.reset" -> "latest",
  "enable.auto.commit" -> (false: java.lang.Boolean)
)

val ssc = new StreamingContext(sc, Seconds(10))

val topics = Array("Hello-Kafka")
val stream = KafkaUtils.createDirectStream[String, String](
  ssc,
  PreferConsistent,
  Subscribe[String, String](topics, kafkaParams)
)

val mapped = stream.map(record => (record.key, record.value))
val wc = mapped.map(x => x._2).flatMap(line => line.split("\\s")).map(word => (word, 1)).reduceByKey(_+_)

val updateFunc = (values: Seq[Int], state: Option[Int]) => {

    val currentCount = values.foldLeft(0)(_ + _)
    val previousCount = state.getOrElse(0)

    Some(currentCount + previousCount)
}

val cumulativeWc = wc.updateStateByKey[Int](updateFunc)

// cumulativeWc.print()
cumulativeWc.foreachRDD { rdd =>
    rdd.foreachPartition { partitionOfRecords =>
        partitionOfRecords.foreach(println)
    }
    println("=======================")
}

ssc.checkpoint("./tmp")
ssc.start()
ssc.awaitTermination()

