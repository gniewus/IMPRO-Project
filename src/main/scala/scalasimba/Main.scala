package main.scala.scalasimbas

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.simba.SimbaSession

object Main {
  case class PointData(x: Double, y: Double, z: Double, other: String)

  def main(args: Array[String]) {
    println("Hello World")
//
    val simbaSession = SimbaSession
      .builder()
      .master("local[4]")
      .appName("SparkSessionForSimba")
      .config("simba.join.partitions", "20")
      .getOrCreate()

    runRangeQuery(simbaSession)
    //Create a SparkContext to initialize Spark
    /* val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Word Count")
    val sc = new SparkContext(conf)

    // Load the text into a Spark RDD, which is a distributed representation of each line of text
    val textFile = sc.textFile("src/main/resources/test.txt")

    //word count
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    counts.foreach(println)
    System.out.println("Total words: " + counts.count());
    */
    //counts.saveAsTextFile("/tmp/shakespeareWordCount")
  }
  private def runRangeQuery(simba: SimbaSession): Unit = {

    System.out.println("Running Range Queries...")

    import simba.implicits._
    val caseClassDS = Seq(PointData(1.0, 1.0, 3.0, "1"),  PointData(2.0, 2.0, 3.0, "2"), PointData(2.0, 2.0, 3.0, "3"),
      PointData(2.0, 2.0, 3.0, "4"),PointData(3.0, 3.0, 3.0, "5"),PointData(4.0, 4.0, 3.0, "6")).toDS()

    import simba.simbaImplicits._
    caseClassDS.range(Array("x", "y"),Array(1.0, 1.0),Array(3.0, 3.0)).show(10)

  }

}
