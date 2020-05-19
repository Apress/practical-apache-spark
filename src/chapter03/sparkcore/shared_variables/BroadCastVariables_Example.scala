package chapter03.sparkcore.shared_variables

import chapter03.sparkcore.PracticalSparkContext

object BroadCastVariables_Example {
  def main(args: Array[String]): Unit = {
    val sc = new PracticalSparkContext().getSparkContext();
    val broadcastVar = sc.broadcast(Array(1, 2, 3)) //Line 1
    print(broadcastVar.value.foreach(println));
  }
}
