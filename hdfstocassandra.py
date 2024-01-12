from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col

def main():
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("HDFSToCassandra") \
        .getOrCreate()

    # Read data from HDFS
    rdd = spark.sparkContext.textFile("hdfs:///user/maria_dev/cassandra/president_county_candidate.csv")

    # Assuming CSV format and converting RDD to DataFrame
    def parse_line(line):
        parts = line.split(",")
        return Row(state=parts[0], county=parts[1], candidate=parts[2],
                   party=parts[3], total_votes=int(parts[4]), won=parts[5])

    row_rdd = rdd.map(parse_line)
    df = spark.createDataFrame(row_rdd)

    # Write the DataFrame to Cassandra
    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode('append') \
        .options(table="elections", keyspace="electiondata") \
        .save()

    spark.stop()

if __name__ == "__main__":
    main()
