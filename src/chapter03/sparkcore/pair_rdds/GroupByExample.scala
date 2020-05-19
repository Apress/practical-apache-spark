package chapter03.sparkcore.pair_rdds

import chapter03.sparkcore.PracticalSparkContext

object GroupByExample {
  def main(args: Array[String]): Unit = {
    
    val sc = new PracticalSparkContext().getSparkContext();

    val rdd = sc.textFile("InputFiles/people.csv") //Line 1

    val splitRdd = rdd.filter(line => !line.contains("year")).map(line => line.split(",")) //Line 2

    val fieldRdd = splitRdd.map(f => (f(2), f(1))) //Line 3

    val groupNamesByCountry = fieldRdd.groupByKey //Line 4

    groupNamesByCountry.foreach(println) //Line 5

  }
}