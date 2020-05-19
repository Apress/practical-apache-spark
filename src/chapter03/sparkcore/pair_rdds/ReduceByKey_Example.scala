package chapter03.sparkcore.pair_rdds

import chapter03.sparkcore.PracticalSparkContext

object ReduceByKey_Example {
  def main(args: Array[String]): Unit = {
    val sc = new PracticalSparkContext().getSparkContext();

    val rdd = sc.textFile("InputFiles/people.csv") //Line 1

    val splitRdd = rdd.filter(line => !line.contains("year")).map(line => line.split(",")) //Line 2

    val fieldRdd = splitRdd.map(f => (f(1), f(3).toInt)) //Line 3

    val namesCount = fieldRdd.reduceByKey((v1, v2) => v1 + v2) //Line 4

    namesCount.foreach(println) //Line 5

    print();
  }
}
