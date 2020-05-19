package chapter07.spark_kafka

import org.apache.spark.sql.{ Row, SparkSession }

import org.apache.spark._

import org.apache.spark.streaming._

import org.apache.spark.streaming.kafka._

object SparkKafka_StructuredStreaming {

  def main(args: Array[String]) {

    // Create Spark Session and Spark Context

    val spark = SparkSession.builder.appName(getClass.getSimpleName).getOrCreate()

    // Get the Spark context from the Spark session to create streaming context

    val sc = spark.sparkContext
    import spark.implicits._
    val readData = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "sparkandkafkatest").load()
    val Ds = readData.selectExpr("CAST(key AS STRING)", "CAST( value AS STRING)").as[(String, String)];
    val wordCounts = Ds.map(_._2.split(" ")).groupBy("value").count()
    val query = wordCounts.writeStream.outputMode("complete").format("console").start()
  }
}