package chapter03.sparkcore.transformations

import chapter03.sparkcore.PracticalSparkContext

/*Objective: To illustrate map(func) tranformation.

Action: Create an RDD of numeric list. Then apply map(func) to multiply each element with 2.
*/
object Map_Example {

  def main(args: Array[String]): Unit = {
    val sc = new PracticalSparkContext().getSparkContext();
    val mapRdd = sc.parallelize(List(1, 2, 3, 4)) // Line 1
    val mapRdd1 = mapRdd.map(x => x * 2)
    mapRdd1.foreach(println)
  }

}