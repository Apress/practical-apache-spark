package chapter03.sparkcore

/*
Objective: To create an RDD using external datasource.
Input File: keywords.txt
Action: Use textFile() method of SparkContext. Specify url path of the local file system as an argument to the textFile() method.
*/

object CreatingRDDFromFile {
  def main(args: Array[String]): Unit = {
    val sc = new PracticalSparkContext().getSparkContext();
    val rdd1 = sc.textFile("InputFiles/keywords.txt")
    rdd1.foreach(println)
  }
  
  // RDD can be created from external data source as well.
  // val rdd = sc.textFile("hdfs://localhost:9000/data/keywords.txt") // RDD from HDFS source
}