package chapter03.sparkcore

object CreatingRDDWithPartitions {
  def main(args: Array[String]): Unit = {
    val sc = new PracticalSparkContext().getSparkContext();
    // the number of partition is 3. File will be divided into 3 partitions.
    val rdd1 = sc.textFile("InputFiles/keywords.txt", 3)
    rdd1.foreach(println)
  }
}