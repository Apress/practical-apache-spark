package chapter06.structured_streaming

import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

case class Student(studId: String, studName: String, grade: Integer);

object StreamingDataFrames {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Spark Structured Example").master("local[*]").getOrCreate()

    import spark.implicits._

    val studId = StructField("studId", DataTypes.StringType)

    val studName = StructField("studName", DataTypes.StringType)

    val grade = StructField("grade", DataTypes.IntegerType)

    val fields = Array(studId, studName, grade)

    val schema = StructType(fields)

    

    // Create Dataset

    val csvDS = spark.readStream.option("sep", ",").schema(schema).csv("InputFiles/Student.csv").as[Student];

    // Select the student names where grade is more than 90

    val studNames = csvDS.select("studName").where("grade>90")

    val query = studNames.writeStream.outputMode("append").format("console").start()

    /*csvDS.createOrReplaceTempView("student")

    val student = spark.sql("select * from student")

    val query = student.writeStream.outputMode("append").format("console").start()*/

    /*val gradeMax = spark.sql("select max(grade) from student")

    val query = gradeMax.writeStream.outputMode("complete").format("console").start()*/
  }
}