package chapter03.sparkcore.transformations

import chapter03.sparkcore.PracticalSparkContext

object Union_Example {
  def main(args: Array[String]): Unit = {
    val sc = new PracticalSparkContext().getSparkContext();
    val rdd = sc.parallelize(1 to 5) //Line 1
    val rdd1 = sc.parallelize(6 to 10) //Line 2
    val unionRdd = rdd.union(rdd1) //Line 3
    unionRdd.foreach(println);
  }
}