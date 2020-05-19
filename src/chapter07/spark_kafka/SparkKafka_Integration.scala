package chapter07.spark_kafka

import org.apache.spark.sql.{ Row, SparkSession }

import org.apache.spark._

import org.apache.spark.streaming._

import org.apache.spark.streaming.kafka._

// wordcount operation
object SparkKafka_Integration {

  def main(args: Array[String]) {

    // Create Spark Session and Spark Context

    val spark = SparkSession.builder.appName(getClass.getSimpleName).getOrCreate()

    // Get the Spark context from the Spark session to create streaming context

    val sc = spark.sparkContext

    // Create the streaming context, interval is 40 seconds

    val ssc = new StreamingContext(sc, Seconds(40))

    // Create Kafka DStream that receives text data from the Kafka Server.

    val kafkaStream = KafkaUtils.createStream(ssc, "localhost:2181", "spark-streaming-consumer-group", Map("sparkandkafkatest" -> 1))

    val words = kafkaStream.flatMap(x => x._2.split(" "))

    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

    // To print the wordcount result of the stream

    wordCounts.print()

    ssc.start()

    ssc.awaitTermination()

  }

}