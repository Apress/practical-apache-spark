package chapter03.sparkcore

/*
Objective: To create an RDD.
Action: Use parallelize method of SparkContext. Create Array of Integer and pass that as an argument to parallieze method.
*/

object CreatingRDD {

  def main(args: Array[String]): Unit = {
    val sc = new PracticalSparkContext().getSparkContext();
    val input = Array(1, 2, 3, 4, 5)
    val rdd1 = sc.parallelize(input)
    rdd1.foreach(println)
  }
}