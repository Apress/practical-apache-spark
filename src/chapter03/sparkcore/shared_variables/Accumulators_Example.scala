package chapter03.sparkcore.shared_variables

import chapter03.sparkcore.PracticalSparkContext

object Accumulators_Example {
  def main(args: Array[String]): Unit = {
    val sc = new PracticalSparkContext().getSparkContext();
    val accum = sc.longAccumulator("My Counter") //Line 1
    sc.parallelize(Array(10, 20, 30, 40)).foreach(x => accum.add(x)) //Line 2
    print(accum.value);
  }
}