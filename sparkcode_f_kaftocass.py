from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession

def saveToCassandra(rdd):
    if not rdd.isEmpty():
        spark = SparkSession.builder.appName("KafkaToCassandra").getOrCreate()
        df = spark.read.json(rdd)
        df.write \
          .format("org.apache.spark.sql.cassandra") \
          .mode('append') \
          .options(table="your_table", keyspace="your_keyspace") \
          .save()

def main():
    sc = SparkContext(appName="KafkaToCassandra")
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 10)  # 10 second window

    kafkaStream = KafkaUtils.createDirectStream(ssc, ["your_topic"], {"metadata.broker.list": "your_kafka_broker:port"})

    parsed = kafkaStream.map(lambda v: v[1])

    parsed.foreachRDD(saveToCassandra)

    ssc.start()
    ssc.awaitTermination()

if __name__ == "__main__":
    main()
