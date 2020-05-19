package chapter03.sparkcore.pair_rdds

import chapter03.sparkcore.PracticalSparkContext

object SortByKey_Example {
  def main(args: Array[String]): Unit = {
    
    val sc = new PracticalSparkContext().getSparkContext();

    val rdd = sc.textFile("InputFiles/people.csv") //Line 1

    val splitRdd = rdd.filter(line => !line.contains("year")).map(line => line.split(",")) //Line 2

    val fieldRdd = splitRdd.map(f => (f(1), f(3).toInt)).sortByKey() //Line 3

    fieldRdd.foreach(println) //Line 4

    print();
  }
}