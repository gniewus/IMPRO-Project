package main.scala.scalasimbas

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types._


object Main {
  case class PointData(x: Double, y: Double, z: Double, other: String)



  def main(args: Array[String]) {


     // println("Hello World")

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



     /*  Reading CSV file from external source
     *   To do : Read CSV from HDFS source
     */

    // Defining Schema First Approach
    /*
    case class TripFareSchema(Medallion: String, HackLicense: String, VendorId: String,
                              PickupDatetime : String, PaymentType: String, FareAmount: Double, SurCharge : Double, MtaTax:Double
                             , TipAmount : Double , TollsAmount: Double , TotalAmount: Double )

    var tripFareSchema = Encoders.product[TripFareSchema].schema
   */

    // Defining Schema Second Approach
    var tripFareSchema = StructType(Array(

      StructField("Medallion",    StringType, true),
      StructField("HackLicense",  StringType, true),
      StructField("VendorId",  StringType, true),
      StructField("PickupDatetime",  StringType, true),
      StructField("PaymentType",  StringType, true),
      StructField("FareAmount",  DoubleType, true),
      StructField("SurCharge",  DoubleType, true),
      StructField("MtaTax",  DoubleType, true),
      StructField("TipAmount",  DoubleType, true),
      StructField("TollsAmount",  DoubleType, true),
      StructField("TotalAmount",  DoubleType, true)

    ))



    val csVTripFareDS = simba.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .schema(tripFareSchema)
      .load("src/main/resources/trip_fare_10000.csv")

    // To view schemas
    csVTripFareDS.printSchema()

    // To view full tables first 20 rows
    csVTripFareDS.show()

    // To view spcific columns
    csVTripFareDS.select("FareAmount","TipAmount","VendorId","PickupDatetime").show()

    /*
    import simba.implicits._
    val caseClassDS = Seq(PointData(1.0, 1.0, 3.0, "1"),  PointData(2.0, 2.0, 3.0, "2"), PointData(2.0, 2.0, 3.0, "3"),
      PointData(2.0, 2.0, 3.0, "4"),PointData(3.0, 3.0, 3.0, "5"),PointData(4.0, 4.0, 3.0, "6")).toDS()
   */

    //  val rangeTotalAmount = csVTripFareDS.select("Medallion")

    import simba.simbaImplicits._

    // caseClassDS.range(Array("x", "y"),Array(1.0, 1.0),Array(3.0, 3.0)).show(10)

    // Here the resultset will contain only those rows where  25.0 => FareAmount >= 0.0 ,  2.0 => TipAmount >= 0.0
    csVTripFareDS.range(Array("FareAmount", "TipAmount"),Array(0.0, 0.0),Array(25.0, 2.0)).show(10)

  }

}
