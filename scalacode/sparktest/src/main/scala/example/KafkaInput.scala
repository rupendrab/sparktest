package example

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object KafkaInput {

    def main(args: Array[String]) {
        val Array(zkQuorum, group, topic, numThreads) = args
        val conf = new SparkConf().setAppName("KafkaInput")
        val ssc = new StreamingContext(conf, Seconds(10))

        val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> zkQuorum,
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> group,
            "auto.offset.reset" -> "latest",
            "enable.auto.commit" -> (false: java.lang.Boolean)
        )

        val topics = Array(topic)
        val stream = KafkaUtils.createDirectStream[String, String](
            ssc,
            PreferConsistent,
            Subscribe[String, String](topics, kafkaParams)
        )

        val mapped = stream.map(record => (record.key, record.value))
        val wc = mapped.map(x => x._2).flatMap(line => line.split("\\s")).map(word => (word, 1)).reduceByKey(_+_)
        wc.print()
        ssc.start()
        ssc.awaitTermination()
        ssc.stop()
    }
}
