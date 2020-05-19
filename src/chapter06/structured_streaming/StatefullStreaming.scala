package chapter06.structured_streaming

import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.sql.Timestamp

object StatefullStreaming {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Spark Structured Example").master("local[*]").getOrCreate()

    import spark.implicits._

    // Create DataFrame representing the stream of input lines from connection to host:port

    val lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).option("includeTimestamp", true).load()

    // Split the lines into words, retaining timestamps

    val words = lines.as[(String, Timestamp)].flatMap(line => line._1.split(" ").map(word => (word, line._2))).toDF("word", "timestamp")

    // Group the data by window and word and compute the count of each group

    val windowedCounts = words.groupBy(window($"timestamp", "10 minutes", "5 minutes"), $"word").count().orderBy("window")

    // Start running the query that prints the windowed word counts to the console

    val query = windowedCounts.writeStream.outputMode("complete").format("console").option("truncate", "false").start()

    query.awaitTermination()
  }
}