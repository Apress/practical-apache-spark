package chapter03.sparkcore

/*objective: To illustrate filter(func) tranformation.

Action: Create an RDD using external dataset. Apply filter(func) to display the lines that contains a word "Kafka".
*/
import chapter03.sparkcore.PracticalSparkContext

object Filter_Example {
  def main(args: Array[String]): Unit = {
    val sc = new PracticalSparkContext().getSparkContext();
    val filterRdd = sc.textFile("InputFiles/keywords.txt")
    val filterRdd1 = filterRdd.filter(line => line.contains("Kafka"))
    filterRdd1.foreach(println)
  }
}