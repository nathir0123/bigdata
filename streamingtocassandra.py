from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split
from pyspark.sql.types import StringType, IntegerType

def main():
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("TopCandidatesKafkaToCassandra") \
        .config("spark.cassandra.connection.host", "cassandra_host") \
        .getOrCreate()

    # Define the schema based on your CSV structure
    columns = ["state", "county", "candidate", "party", "votes"]

    # Read from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka_server:9092") \
        .option("subscribe", "topic_name") \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse the CSV data from Kafka
    df = df.selectExpr("CAST(value AS STRING)") \
           .select(split(col("value"), ",\s*").alias("data")) \
           .select([col("data").getItem(i).alias(columns[i]) for i in range(len(columns))])

    # Cast 'votes' to Integer
    df = df.withColumn("votes", col("votes").cast(IntegerType()))

    # Calculate top 5 candidates
    top_candidates = df.groupBy("candidate") \
        .sum("votes") \
        .withColumnRenamed("sum(votes)", "total_votes") \
        .orderBy(col("total_votes").desc()) \
        .limit(5)

    # Write the results (names and vote totals of top 5 candidates) to Cassandra
    query = top_candidates.writeStream \
        .outputMode("complete") \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "your_keyspace") \
        .option("table", "your_table") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
