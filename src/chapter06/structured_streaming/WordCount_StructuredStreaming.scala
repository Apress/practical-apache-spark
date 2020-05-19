package chapter06.structured_streaming

import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession

object WordCount_StructuredStreaming {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Spark Structured Example").master("local[*]").getOrCreate()

    import spark.implicits._

    //Create a streaming DataFrame to represent the text data received from a data server listening on TCP (localhost:9999)
    val lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load();
    // Convert line DataFrame into Dataset and split the lines into multiple words

    val words = lines.as[String].flatMap(_.split(" "))
    // Generate running word count

    val wordCounts = words.groupBy("value").count()
    val query = wordCounts.writeStream.outputMode("complete").format("console").start()
    query.awaitTermination()
    
    // Start the netcat server using below command and type few messages and execute the program 
    // netcat -lk 9999


  }
}