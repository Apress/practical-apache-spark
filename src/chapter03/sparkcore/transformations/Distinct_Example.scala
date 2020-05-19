package chapter03.sparkcore.transformations

import chapter03.sparkcore.PracticalSparkContext

object Distinct_Example {
  def main(args: Array[String]): Unit = {
    val sc = new PracticalSparkContext().getSparkContext();
    val rdd = sc.parallelize(10 to 15) //Line 1
    val rdd1 = sc.parallelize(10 to 15) //Line 2
    val distinctRdd = rdd.union(rdd1).distinct //Line 3
    distinctRdd.foreach(println);

  }
}