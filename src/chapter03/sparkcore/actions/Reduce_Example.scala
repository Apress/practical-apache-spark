package chapter03.sparkcore.actions

import chapter03.sparkcore.PracticalSparkContext

object Reduce_Example {
  def main(args: Array[String]): Unit = {
    val sc = new PracticalSparkContext().getSparkContext();
    val rdd = sc.parallelize(1 to 5) //Line 1
    val sumRdd = rdd.reduce((t1, t2) => t1 + t2) //Line 2
    print(sumRdd);
    rdd.collect();  // collect action
  }
}
