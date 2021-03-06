package example

import org.apache.spark._
import org.apache.spark.SparkContext._

object WordCount {

    def main(args: Array[String]) {
        val inputFile = args(0)
        val conf = new SparkConf().setMaster("local[2]").setAppName("wordCount")
        val sc = new SparkContext(conf)
        val input = sc.textFile(inputFile)
        val words = input.flatMap(line => line.split(" "))
        val counts = words.map(word => (word, 1)).reduceByKey{ case(x, y) => x + y }
        counts.collect.foreach(println)
    }
}
