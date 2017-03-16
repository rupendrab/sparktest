## Spark Streaming Case Study

### Use Case:   

Create and update multiple aggregates in Spark from Streaming data created by new files dropped in a specific directory and save the aggregations to a PostgreSQL database


#### Details:  
  1. Run Kafka  
  2. Use a Python Daemon process to publish files line by line to a Kafka topic whenever a new file is added to a directory  
  3. Run Spark Streaming to read from the Kafka topic and create two aggregations:  
        1. Cumulative word count  
        2. Cumulative word count by document name  
  4. In Spark, create the incremental accumulation sets
  5. Update a PostgreSQL database with the incremental data, so the database is always in sync with the aggregates
  
#### OS:
  1. Linux CentOS release 6.8 (Final)
  
#### Software and versions:
  1. Python version 2.7.13 with pykafka 2.5.0 and beautifulsoup4 4.5.3  
      ```
      pip install pykafka  
      pip install beautifulsoup4
      ```  
  2. Kafka version 2.11-0.10.1.1
  3. Spark version 2.1.0-bin-hadoop2.6
  4. PostgreSQL version 9.6.2
  
#### Details:

1. cd to the codebase directory
2. Start PostgreSQL and create the tables and data load functions  
   ```
   su - postgres
   pg_ctl start
   psql -c "create database rupen"
   exit
   psql -d rupen -U postgres
   \i scalacode/sparktest/sql/ddl.sql
   \d word_counts*
   
   \d word_counts*
         Table "public.word_counts"
   Column |          Type          | Modifiers 
  --------+------------------------+-----------
   word   | character varying(100) | not null
   cnt    | integer                | 
  Indexes:
      "pk_word_counts" PRIMARY KEY, btree (word)

       Table "public.word_counts_by_file"
   Column |          Type          | Modifiers 
  --------+------------------------+-----------
   fname  | character varying(100) | not null
   word   | character varying(100) | not null
   cnt    | integer                | 
  Indexes:
      "pk_word_counts_by_file" PRIMARY KEY, btree (fname, word)
   ```
    
