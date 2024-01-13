from pyspark import SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import sum as _sum, desc

def parse_line(line):
    parts = line.split(",")
    # Assuming the first line is a header
    if parts[0] == 'state':
        return None
    return Row(state=parts[0], county=parts[1], candidate=parts[2],
               party=parts[3], total_votes=int(parts[4]), won=parts[5])

def saveToCassandra(df):
    if df.rdd.isEmpty():
        return
    df.write \
      .format("org.apache.spark.sql.cassandra") \
      .mode('append') \
      .options(table="electionn", keyspace="electiondata") \
      .save()

def main():
    sc = SparkContext(appName="HDFSToCassandra")
    sc.setLogLevel("INFO")
    spark = SparkSession.builder.appName("HDFSToCassandra").getOrCreate()

    # Read data from HDFS
    data_rdd = sc.textFile("hdfs:///user/maria_dev/flumev1/*")
    parsed_rdd = data_rdd.map(parse_line).filter(lambda x: x is not None)
    df = spark.createDataFrame(parsed_rdd)

    # Calculate top 5 candidates by total votes
    top_candidates = df.groupBy("candidate") \
                       .agg(_sum("total_votes").alias("all_votes")) \
                       .orderBy(desc("all_votes")) \
                       .limit(5)

    # Show the results in the console
    top_candidates.show()

    # Save to Cassandra
    saveToCassandra(df)

    spark.stop()

if __name__ == "__main__":
    main()
