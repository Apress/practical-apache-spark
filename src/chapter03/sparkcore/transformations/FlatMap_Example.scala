package chapter03.sparkcore

import chapter03.sparkcore.PracticalSparkContext

/*Objective: To illustrate flatMap(func) tranformation.

Action: Create an RDD for list of Strings, apply flatMap(func).
*/

object FlatMap_Example {
  def main(args: Array[String]): Unit = {
    val sc = new PracticalSparkContext().getSparkContext();
    val flatMapRdd = sc.parallelize(List("hello world", "hi"))
    val flatMapRdd1 = flatMapRdd.flatMap(line => line.split(" "))
    flatMapRdd1.foreach(println)
  }
}