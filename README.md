## Spark Streaming Case Study

### Use Case:   

Create and update multiple aggregates in Spark from Streaming data created by new files dropped in a specific directory and save the aggregations to a PostgreSQL database


#### Details:  
  1. Run Kafka  
  2. Use a Python Daemon process to publish files line by line to a Kafka topic whenever a new file is added to a directory  
  3. Run Spark Streaming to read from the Kafka topic and create two aggregations:  
        1. Cumulative word count  
        2. Cumulative word count by document name  
    
