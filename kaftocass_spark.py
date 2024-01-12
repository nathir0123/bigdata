from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession, Row

def saveToCassandra(rdd):
    if not rdd.isEmpty():
        spark = SparkSession.builder.appName("KafkaToCassandra").getOrCreate()
        
        def parse_line(line):
            parts = line.split(",")
            return Row(state=parts[0], county=parts[1], candidate=parts[2],
                       party=parts[3], total_votes=int(parts[4]), won=parts[5])
        
        row_rdd = rdd.map(parse_line)
        df = spark.createDataFrame(row_rdd)
        df.write \
          .format("org.apache.spark.sql.cassandra") \
          .mode('append') \
          .options(table="elections", keyspace="electiondata") \
          .save()

def main():
    sc = SparkContext(appName="KafkaToCassandra")
    sc.setLogLevel("INFO")
    ssc = StreamingContext(sc, 10)  # 10 second window

    kafkaStream = KafkaUtils.createDirectStream(ssc, ["zerotohero"], {"metadata.broker.list": "sandbox-hdp.hortonworks.com:6667"})
    parsed = kafkaStream.map(lambda v: v[1])
    parsed.foreachRDD(saveToCassandra)

    ssc.start()
    ssc.awaitTermination()

if __name__ == "__main__":
    main()
