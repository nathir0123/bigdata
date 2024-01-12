from pyspark import SparkContext
from pyspark.sql import SparkSession, Row

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
      .options(table="elections", keyspace="electiondata") \
      .save()

def main():
    sc = SparkContext(appName="HDFSToCassandra")
    sc.setLogLevel("INFO")
    spark = SparkSession.builder.appName("HDFSToCassandra").getOrCreate()

    # Read data from HDFS
    data_rdd = sc.textFile("hdfs:///user/maria_dev/cassandra/president_county_candidate.csv")
    parsed_rdd = data_rdd.map(parse_line).filter(lambda x: x is not None)
    df = spark.createDataFrame(parsed_rdd)
    
    saveToCassandra(df)

    spark.stop()

if __name__ == "__main__":
    main()
