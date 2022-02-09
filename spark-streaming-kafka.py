from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

sc = SparkContext(appName="PythonSparkStreamingKafka")
sc.setLogLevel("WARN")

ssc = StreamingContext(sc, 10)
directKafkaStream = KafkaUtils.createDirectStream(ssc, ["quickstart-events"], {"metadata.broker.list": "192.168.33.13:9092"})

directKafkaStream.map(lambda x: x[1]).pprint()


#Starting Spark context
ssc.start()
ssc.awaitTermination()
