// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC Mount the AWS data source bucket as a volume that can be accessed by Spark (This particluar code is DataBricks specific)
// MAGIC 
// MAGIC The following code will make the contents of bucket rupen-data on s3 as a directory named /mnt/rupendata

// COMMAND ----------

/*
val AccessKey = "*****"
val SecretKey = "*****"
val EncodedSecretKey = SecretKey.replace("/", "%2F")
val AwsBucketName = "rupen-data"
*/

val MountName = "rupendata"

/*
dbutils.fs.mount(s"s3a://$AccessKey:$EncodedSecretKey@$AwsBucketName", s"/mnt/$MountName")
*/

// COMMAND ----------

// MAGIC %md
// MAGIC * **Load a Text File as RDD** (The Hound of the Baskervilles) from the mounted volume on s3
// MAGIC * Check the number of records

// COMMAND ----------

val myRDD = sc.textFile(s"/mnt/$MountName/hb.txt")
myRDD.cache()
val noLines = myRDD.count()
println(s"Number of lines in book = $noLines")

// COMMAND ----------

// MAGIC %md 
// MAGIC View first 20 lines

// COMMAND ----------

myRDD.take(20).foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC * Split lines into words and use flatMap to create one record per word

// COMMAND ----------

val words = myRDD.flatMap(x => x.split("\\W+")) // \W+ is regular expression for non-word character
words.take(10).foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC * Filter out blanks

// COMMAND ----------

val wordsNoBlanks = words.filter(word => ! word.matches("^\\s*$"))
wordsNoBlanks.take(10).foreach(x => println(x))

// COMMAND ----------

// MAGIC %md
// MAGIC * Convert each word to lower case to avoid double counting using map
// MAGIC * Count frequency of each word using reduceByKey
// MAGIC * Cache the counts for quickwe subsequent analysis

// COMMAND ----------

val wordCounts = ( wordsNoBlanks
                      .map(word => (word.toLowerCase(),1))
                      .reduceByKey(_ + _)
                 )
wordCounts.take(10).foreach(println)
wordCounts.cache()

// COMMAND ----------

// MAGIC %md
// MAGIC * Explore most frequent words

// COMMAND ----------

val wordCountsSortedByMostFrequent = 
(wordCounts
  .map(data => data match { case (word, count) => (count, word)})
  .sortByKey(false)
  .map(data => data match { case (count, word) => (word, count)})
)
wordCountsSortedByMostFrequent.take(30).foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC * Explore least frequent words

// COMMAND ----------

val wordCountsSortedByLeastFrequent = 
(wordCounts
  .map(data => data match { case (word, count) => (count, word)})
  .sortByKey(true)
  .map(data => data match { case (count, word) => (word, count)})
)
wordCountsSortedByLeastFrequent.take(30).foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC * Save results back to s3

// COMMAND ----------

val outputDirectory = s"/mnt/$MountName/counts1"
// dbutils.fs.rm(outputDirectory, true)
wordCountsSortedByLeastFrequent.saveAsTextFile(outputDirectory)

// COMMAND ----------

// MAGIC %md
// MAGIC * Check the output files using dbutils

// COMMAND ----------

display(dbutils.fs.ls(outputDirectory))

// COMMAND ----------

dbutils.fs.help()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Explore the DataFrame API
// MAGIC   * Comma-Delimited csv files
// MAGIC   * JSON Files

// COMMAND ----------

// MAGIC %md
// MAGIC ###### Load Data to DataFrame
// MAGIC 
// MAGIC * zips.json can be downloaded from [http://media.mongodb.org/zips.json?_ga=1.1060534.640622194.1485360367]
// MAGIC * The content of file bodies.csv is below:  
// MAGIC 
// MAGIC id,width,height,depth,material,color  
// MAGIC 1,1.0,1.0,1.0,wood,brown  
// MAGIC 2,2.0,2.0,2.0,glass,green  
// MAGIC 3,3.0,3.0,3.0,metal,blue  

// COMMAND ----------

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

val bodiesRDD = loadFromS3(MountName, "bodies.csv")
val jsondataRDD = loadFromS3(MountName, "zips.json")

// COMMAND ----------

bodiesRDD.collect().foreach(println)

// COMMAND ----------

jsondataRDD.cache()
println(jsondataRDD.count)
jsondataRDD.take(10).foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC * Load the CSV file into a DataFrame

// COMMAND ----------

val bodiesDF = spark.read
        .format("csv")
        .option("header", "true") //reading the headers
        .option("mode", "DROPMALFORMED")
        .csv(s"/mnt/$MountName/bodies.csv")
bodiesDF.show()
bodiesDF.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC * Use a custom schema definition for the DataFrame

// COMMAND ----------

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

// MAGIC %md
// MAGIC * Convert the JSON RDD into DataFrame

// COMMAND ----------

val zipsDF = sqlContext.read.json(jsondataRDD)
zipsDF.show(10)
zipsDF.printSchema()
zipsDF.cache() /* No need to make the trip to S3 again */

// COMMAND ----------

// MAGIC %md
// MAGIC ###### Analyze data in DataFrame

// COMMAND ----------

// MAGIC %md
// MAGIC * Count and Filter

// COMMAND ----------

// Count
println("Count : " + zipsDF.count() + "\n")

// Filter by population >= 40000
zipsDF.filter(zipsDF.col("pop") >= 40000).show(10)


// COMMAND ----------

// MAGIC %md
// MAGIC * Group By and Order By  
// MAGIC   Display number of records per state order by state

// COMMAND ----------

zipsDF
  .groupBy($"state")
  .count()
  .orderBy("state")
  .show(100)

// COMMAND ----------

// MAGIC %md
// MAGIC * Specify an aggregation function in Group By  
// MAGIC   Get total population by State

// COMMAND ----------

zipsDF
  .groupBy($"state")
  .agg(Map("pop" -> "sum"))
  .show()

// COMMAND ----------

// MAGIC %md
// MAGIC * More complex query achieved by chaining  
// MAGIC   Display cities that have more than 20 zipcodes  
// MAGIC   1. Group by city and state
// MAGIC   2. Count records per group
// MAGIC   3. Filter by count > 20
// MAGIC   4. Order by count in descending order
// MAGIC   5. Use the display feature of the notebook
// MAGIC   

// COMMAND ----------

display(
  zipsDF
    .groupBy($"city", $"state")
    .count()
    .filter($"count" > 20)
    .orderBy($"count" desc)
)

// COMMAND ----------

// MAGIC %md
// MAGIC ###### Use SparkSQL on DataFrame

// COMMAND ----------

// MAGIC %md
// MAGIC * Register a view for the dataframe

// COMMAND ----------

zipsDF.createOrReplaceTempView("zips")

// COMMAND ----------

// MAGIC %md
// MAGIC Use SQL to display total population for a specific zip code

// COMMAND ----------

display(spark.sql("select _id as postal_code, sum(pop) as population from zips group by _id having _id = '20878'"))

// COMMAND ----------

// MAGIC %md
// MAGIC Number of Zip Codes by State

// COMMAND ----------

spark.sql("select state, count(*) as cnt from zips group by state order by state").show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC ###### More complex queries

// COMMAND ----------

// MAGIC %md
// MAGIC **Requirement**:  
// MAGIC 
// MAGIC Display state, city, state population, city population and percentage of state population accounted for by the city  
// MAGIC Steps:   
// MAGIC * Compute a DataFrame for population by state
// MAGIC * Compute a DataFrame for population by city
// MAGIC * Register these as SQL views and cache them

// COMMAND ----------

val popByState = spark.sql("select state, count(*) as cnt_city, sum(pop) as total_population from zips group by state order by state")
val popByCity = spark.sql("select state, city, sum(pop) as city_population from zips group by state, city order by state, city")
popByState.createOrReplaceTempView("state")
popByCity.createOrReplaceTempView("city")
popByCity.cache()
popByState.cache()
popByState.show(10)
popByCity.show(10)


// COMMAND ----------

// MAGIC %md
// MAGIC Steps (continued):   
// MAGIC * Join the city and state views to get the desired results

// COMMAND ----------

val combined = spark.sql("""
select city.state, city.city, state.total_population as state_population, city.city_population,
       round(city.city_population * 100.0 / state.total_population, 2) as pct_population 
from   city 
join state on state.state = city.state order by city.state, city.city
""");
combined.cache();
println(combined.count())

// COMMAND ----------

// MAGIC %md
// MAGIC __We can also code the entire logic in a single sql with embedded subqueries__

// COMMAND ----------

val combined2 = spark.sql("""
select city.state, 
       city.city, 
       state.total_population as state_population, 
       city.city_population,
       round(city.city_population * 100.0 / state.total_population, 2) as pct_population 
from   (
         select state, 
                city, 
                sum(pop) as city_population 
         from   zips 
         group by 
                state, 
                city 
         order by 
                state, 
                city
       ) as city 
join   (
         select state, 
                count(*) as cnt_city, 
                sum(pop) as total_population 
         from   zips 
         group by 
                state 
         order by 
                state
       ) as state on state.state = city.state 
order by 
      city.state, 
      city.city
""");
combined2.cache();
println(combined2.count())

// COMMAND ----------

// MAGIC %md
// MAGIC Review the execution plan

// COMMAND ----------

combined2.explain

// COMMAND ----------

// MAGIC %md
// MAGIC * Use the derived data to check the most populated cities of a given state

// COMMAND ----------

combined2
  .filter($"state" === "NM")
  .orderBy($"pct_population".desc)
  .show(30)
/*
combined2
  .select("city_population")
  .groupBy()
  .sum()
  .show()
*/

// COMMAND ----------

// MAGIC %md
// MAGIC __Analytic Functions__  
// MAGIC Show most populated cities of each state order by percentage of state population in descending order

// COMMAND ----------

combined2.createOrReplaceTempView("city_state_summary")
val city_rank_by_state = spark.sql("""
select state,
       city,
       state_population,
       city_population,
       pct_population,
       rank() over(partition by state order by pct_population desc) city_rank_in_state
from   city_state_summary
""")
city_rank_by_state.filter($"city_rank_in_state" === 1).orderBy($"pct_population".desc).show()


// COMMAND ----------

// MAGIC %md
// MAGIC ##### View data on a map

// COMMAND ----------

display(popByState)
