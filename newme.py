from pyspark import SparkContext
from pyspark.sql import SparkSession, Row

def parse_line(line):
    parts = line.split(",")
    return Row(state=parts[0], county=parts[1], candidate=parts[2],
               party=parts[3], total_votes=int(parts[4]), won=parts[5])

def saveToCassandra(rdd):
    if not rdd.isEmpty():
        spark = SparkSession.builder.appName("HDFSToCassandra").getOrCreate()
        row_rdd = rdd.map(parse_line)
        df = spark.createDataFrame(row_rdd)
        df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode('append') \
            .options(table="elections", keyspace="electiondata") \
            .save()

def main():
    sc = SparkContext(appName="HDFSToCassandra")
    sc.setLogLevel("INFO")
    spark = SparkSession(sc)

    # Read data from HDFS, assuming the first line is a header
    rdd = spark.sparkContext.textFile("hdfs:///path/to/your/data.csv")
    header = rdd.first()  # extract the header
    data_rdd = rdd.filter(lambda line: line != header)  # filter out the header

    data_rdd.foreachRDD(saveToCassandra)

    spark.stop()

if __name__ == "__main__":
    main()
