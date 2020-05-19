package chapter03.sparkcore.transformations

import chapter03.sparkcore.PracticalSparkContext

object Intersection_Example {
  def main(args: Array[String]): Unit = {
    val sc = new PracticalSparkContext().getSparkContext();
    val rdd = sc.parallelize(1 to 5) //Line 1
    val rdd1 = sc.parallelize(1 to 2) //Line 2
    val intersectionRdd = rdd.intersection(rdd1) //Line 3
    intersectionRdd.foreach(println);
  }
}