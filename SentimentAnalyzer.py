import sys
import os
from pyspark.streaming import StreamingContext
from pyspark import SparkContext
from pyspark.streaming.kafka import KafkaUtils
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from elasticsearch import Elasticsearch

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

es = Elasticsearch(['https://ElasticsearchURL:9200'])


def sentiment(text):
    sentimentAnalyser = SentimentIntensityAnalyzer()
    sentiment = sentimentAnalyser.polarity_scores(text)
    if(sentiment["compound"] > 0):
        return "positive"
    elif(sentiment["compound"] < 0):
        return "negative"
    else:
        return "neutral"


def getSentiment(time, rdd):
    test = rdd.collect()
    for i in test:
        es.index(index="hash_tags_sentiment_analysis",
                 doc_type="tweet-sentiment-analysis", body=i)


if __name__ == "__main__":
    sc = SparkContext.getOrCreate()
    ssc = StreamingContext(sc, 20)

    brokers = 'localhost:9092'
    topic = ["BLM"]
    kvs = KafkaUtils.createDirectStream(
        ssc, topic, {"metadata.broker.list": brokers})
    tweets = kvs.map(lambda x: str(x[1].encode("ascii", "ignore"))).map(
        lambda x: (x, sentiment(x), "#BLM")).map(lambda x: {"message": x[0], "sentiment": x[1], "hashTag": x[2]})
    tweets.foreachRDD(getSentiment)
    ssc.start()
    ssc.awaitTermination()
