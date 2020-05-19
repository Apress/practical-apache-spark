package chapter03.sparkcore.wordcount

import org.apache.spark.{ SparkContext, SparkConf }
import chapter03.sparkcore.PracticalSparkContext

object WordCountOperation {

  def main(args: Array[String]) {

    val sc = new PracticalSparkContext().getSparkContext();

    val inputFileName = "InputFiles/wordcount.txt"

    val outputFileName = "InputFiles/WordCountOutput"

    val wc = sc.textFile(inputFileName)

      .flatMap(line => line.split(" "))

      .map(word => (word, 1))

      .reduceByKey(_ + _);

    wc.foreach(println)

    //wc .saveAsTextFile(outputFileName)

  }

}