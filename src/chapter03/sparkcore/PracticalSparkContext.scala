package chapter03.sparkcore


import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

class PracticalSparkContext {
  def getSparkContext(): SparkContext = {
    val conf = new SparkConf().setAppName("practicalSpark").setMaster("local[*]");
    val sc = new SparkContext(conf);
    return sc;
  }
}