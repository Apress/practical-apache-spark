package chapter03.sparkcore.transformations

import chapter03.sparkcore.PracticalSparkContext

object MapPartitionWithIndex_Example {
  def main(args: Array[String]): Unit = {
    val sc = new PracticalSparkContext().getSparkContext();
    val rdd = sc.parallelize(1 to 5, 2) // Line 1
    val result = rdd.mapPartitionsWithIndex((index: Int, it: Iterator[Int]) => it.toList.map(x => index + ", " + x).iterator) //Line 2
    result.foreach(println);
  }
}

