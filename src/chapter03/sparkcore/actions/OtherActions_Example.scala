package chapter03.sparkcore.actions

import chapter03.sparkcore.PracticalSparkContext

object OtherActions_Example {
  def main(args: Array[String]): Unit = {
    val sc = new PracticalSparkContext().getSparkContext();
    val rdd = sc.parallelize(List("Hello Spark", "Spark Programming")) //Line 1
    print(rdd.count());

    print(rdd.first());

    val courseRdd = sc.parallelize(List("Scala", "Spark", "Spark SQL", "MLib"))

    print("first 2 elements : " + courseRdd.take(2))

    //saveAsTextFile(path): write the elements of the RDD as a text file in local file system or HDFS or other storage system.
    courseRdd.saveAsTextFile("/home/data/output")

    // Apply foreach(func) to print each element in the RDD.
    print("Iterating elements in RDD")
    courseRdd.foreach(println)
  }
}
