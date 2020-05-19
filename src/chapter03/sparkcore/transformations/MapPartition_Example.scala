package chapter03.sparkcore.transformations

import chapter03.sparkcore.PracticalSparkContext

/*It is similar to map, but works on partition level
  Objective: To illustrate mapPartitions(func) tranformation.
  Action: Create an RDD of numeric type. Apply mapPartition(func)
*/

object MapPartition_Example {
  def main(args: Array[String]): Unit = {
    val sc = new PracticalSparkContext().getSparkContext();
    val rdd = sc.parallelize(10 to 90)
    val mapped = rdd.mapPartitions(x => List(x.next).iterator)
    mapped.foreach(println)
  }
}