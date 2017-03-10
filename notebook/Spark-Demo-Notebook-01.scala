// Databricks notebook source
/*
Mount the AWS data source bucket as a volume that can be accessed by Spark (This particluar code is DataBricks specific)

The following code will make the contents of bucket rupen-data on s3 as a directory named /mnt/rupendata
*/

val AccessKey = "*****"
val SecretKey = "*****"
val EncodedSecretKey = SecretKey.replace("/", "%2F")
val AwsBucketName = "rupen-data"
val MountName = "rupendata"

dbutils.fs.mount(s"s3a://$AccessKey:$EncodedSecretKey@$AwsBucketName", s"/mnt/$MountName")

// COMMAND ----------

/*

Run a data discovery followed by a Word Count program

*/

val myRDD = sc.textFile(s"/mnt/$MountName/hb.txt")
val noLines = myRDD.count()
println(s"Number of lines in book = $noLines")

// COMMAND ----------

/*
  Split lines to individual words
*/

val words = myRDD.flatMap(x => x.split("\\W+"))
words.take(10).foreach(println)

/*
val wordsNoBlanks = words.filter(word => ! word.matches("^\\s*$"))
wordsNoBlanks.take(10).foreach(x => println(x))
*/

// COMMAND ----------

/*
  Count word frequency
*/

val wordCounts = ( wordsNoBlanks
                      .map(word => (word.toLowerCase(),1))
                      .reduceByKey(_ + _)
                 )
wordCounts.take(10).foreach(println)
wordCounts.cache()

// COMMAND ----------

/*
  Explore most frequent words
*/

val wordCountsSortedByMostFrequent = 
(wordCounts
  .map(data => data match { case (word, count) => (count, word)})
  .sortByKey(false)
  .map(data => data match { case (count, word) => (word, count)})
)
wordCountsSortedByMostFrequent.take(30).foreach(println)

// COMMAND ----------

/*
  Explore least frequent words
*/

val wordCountsSortedByLeastFrequent = 
(wordCounts
  .map(data => data match { case (word, count) => (count, word)})
  .sortByKey(true)
  .map(data => data match { case (count, word) => (word, count)})
)
wordCountsSortedByLeastFrequent.take(30).foreach(println)

// COMMAND ----------

/*
   Save the results back to S3
*/

wordCountsSortedByLeastFrequent.saveAsTextFile(s"/mnt/$MountName/counts1")

// COMMAND ----------

/*
    Explore DataSets API with comma-delimited and JSON data.
*/

def loadFromWebFile(URL: String) = {
  sc.parallelize(scala.io.Source.fromURL(URL).mkString.split("\n"))
}

def loadFromS3(MountName: String, fileName: String) = {
  sc.textFile(s"/mnt/$MountName/$fileName")
}

/*
val bodies = loadFromWebFile("http://rupen-data.s3-website-us-east-1.amazonaws.com/bodies.csv");
val jsondatardd = loadFromWebFile("http://rupen-data.s3-website-us-east-1.amazonaws.com/zips.json");
*/

val bodies = loadFromS3(MountName, "bodies.csv")
val jsondatardd = loadFromS3(MountName, "zips.json")
bodies.collect().foreach(println)

// COMMAND ----------

/*
    Load DataSet from CSV File
*/

val bodiesDF = spark.read
        .format("csv")
        .option("header", "true") //reading the headers
        .option("mode", "DROPMALFORMED")
        .csv(s"/mnt/$MountName/bodies.csv")
bodiesDF.show()
bodiesDF.printSchema()

// COMMAND ----------

/*
  Specify a schema
*/

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StringType

val schema = StructType(
        Array(
            StructField("id", IntegerType, true), 
            StructField("width", DoubleType, true),
            StructField("height", DoubleType, true),
            StructField("depth", DoubleType, true),
            StructField("material", StringType, true),
            StructField("color", StringType, true)
        )
    )

val bodiesDF = spark.read
        .format("csv")
        .option("header", "true") //reading the headers
        .option("mode", "DROPMALFORMED")
        .schema(schema)
        .csv(s"/mnt/$MountName/bodies.csv")
bodiesDF.show()
bodiesDF.printSchema()

// COMMAND ----------

/*
    Load data in JSON format
*/

jsondatardd.take(10).foreach(println)
val zipsDF = sqlContext.read.json(jsondatardd)
zipsDF.show(10)
zipsDF.cache() /* No need to make the trip to S3 again */

// COMMAND ----------

/*
    Explore using the DataSet APIs
*/

// Count
println("Count : " + zipsDF.count() + "\n")

// Filter by population >= 40000
zipsDF.filter(zipsDF.col("pop") >= 40000).show(10)


// COMMAND ----------

/*
    Group by State and Order by State
*/

zipsDF.groupBy($"state").count().orderBy("state").show(100)/

// COMMAND ----------

/*
    Specify an aggregation function
*/
zipsDF.groupBy($"state").agg(Map("city" -> "count")).show()

// COMMAND ----------

/*
    Chained commands
        Counts by City and State where Count > 20 and show the higher values first
        We also explore the display function of the nodebook to show data graphically
*/

display(zipsDF.groupBy($"city", $"state").count().filter($"count" > 20).orderBy($"count" desc))

// COMMAND ----------

/*
    SparkSQL
    Create a spark view to run SQL queries
*/

zipsDF.createOrReplaceTempView("zips")

// COMMAND ----------

/*
   Total population for a specific Zip Code
*/

display(spark.sql("select _id as postal_code, sum(pop) as population from zips group by _id having _id = '20878'"))

// COMMAND ----------

spark.sql("select state, count(*) as cnt from zips group by state order by state").show(100)

// COMMAND ----------

/*
    Create multiple views of population:
    1. Population by State
    2. Population by State and City
    3. Cache the results
*/
val popByState = spark.sql("select state, count(*) as cnt_city, sum(pop) as total_population from zips group by state order by state")
val popByCity = spark.sql("select state, city, sum(pop) as city_population from zips group by state, city order by state, city")
popByState.createOrReplaceTempView("state")
popByCity.createOrReplaceTempView("city")
popByCity.cache()
popByState.cache()
popByState.show(10)
popByCity.show(10)


// COMMAND ----------

/*
    Combine the views to show total population by state and city with every record and compute
    percentage of state population per city
*/

val combined = spark.sql("""
select city.state, city.city, state.total_population as state_population, city.city_population,
       round(city.city_population * 100.0 / state.total_population, 2) as pct_population 
from   city 
join state on state.state = city.state order by city.state, city.city
""");
combined.cache();
println(combined.count())

// COMMAND ----------

/*
    Explore the view for the most populated cities of Colorado
*/

combined.filter($"state" === "CO").orderBy($"pct_population" desc).show()

// COMMAND ----------

/*
    Explore mapping capabilities of the nodebook
*/

display(popByState)

// COMMAND ----------


