import org.apache.spark.sql.simba.examples.BasicSpatialOps.PointData
import org.apache.spark.sql.simba.{Dataset, SimbaSession}
import org.apache.spark.sql.simba.index.{RTreeType, TreapType}

/**
  * Created by tomasztkaczyk on 27.05.18.
  */
object NY_Example {

  case class PointData(x: Double, y: Double, z: Double, other: String)

  def main(args: Array[String]): Unit = {

    val simbaSession = SimbaSession
      .builder()
      .master("local[4]")
      .appName("SparkSessionForSimba")
      .config("simba.join.partitions", "20")
      .getOrCreate()

    loadSampleData(simbaSession, "trip_10000")
    //runRangeQuery(simbaSession)
    //runKnnQuery(simbaSession)
    //runJoinQUery(simbaSession)
    simbaSession.stop()
  }

  private def loadSampleData(simba: SimbaSession, file_name: String): Unit = {
    import scala.io
    val bufferedSource = io.Source.fromFile("./" + file_name + ".csv")
    println(bufferedSource)
    val AirportPassenger = PointData(40.772063, -73.872846,1.0, "Test")
    var ctr = 0;
    var taxis = Seq()
    for (line <- bufferedSource.getLines) {
      val cols = line.split(",").map(_.trim)
      // do whatever you want with the columns here
      if (ctr > 0){
        val len =  cols.length
        var dropOffLong = cols(len-2)
        var dropOffLat = cols(len-1)

        val P = PointData(dropOffLat.toDouble,dropOffLat.toDouble,1.0,"taxi"+ctr.toString)

        println(s"${cols(len-4)}|${cols(len-3)}|${cols(len-2)}|${cols(len-1)}")
        println(P.toString)

      }

      ctr = ctr +1;
    };



    import simba.simbaImplicits._
    //caseClassDS.range(Array("x", "y"), Array(1.0, 1.0), Array(3.0, 3.0)).show(10)



  }

  private def runKnnQuery(simba: SimbaSession): Unit = {

    import simba.implicits._


    val caseClassDS = Seq(PointData(1.0, 1.0, 3.0, "1"), PointData(2.0, 2.0, 3.0, "2"), PointData(2.0, 2.0, 3.0, "3"),
      PointData(2.0, 2.0, 3.0, "4"), PointData(3.0, 3.0, 3.0, "5"), PointData(4.0, 4.0, 3.0, "6")).toDS()

    import simba.simbaImplicits._
    caseClassDS.knn(Array("x", "y"), Array(1.0, 1.0), 4).show(4)

  }

  private def runRangeQuery(simba: SimbaSession): Unit = {

    import simba.implicits._
    val caseClassDS = Seq(PointData(1.0, 1.0, 3.0, "1"), PointData(2.0, 2.0, 3.0, "2"), PointData(2.0, 2.0, 3.0, "3"),
      PointData(2.0, 2.0, 3.0, "4"), PointData(3.0, 3.0, 3.0, "5"), PointData(4.0, 4.0, 3.0, "6")).toDS()

    import simba.simbaImplicits._
    caseClassDS.range(Array("x", "y"), Array(1.0, 1.0), Array(3.0, 3.0)).show(10)

  }

  private def runJoinQUery(simba: SimbaSession): Unit = {

    import simba.implicits._

    val DS1 = (0 until 10000).map(x => PointData(x, x + 1, x + 2, x.toString)).toDS
    val DS2 = (0 until 10000).map(x => PointData(x, x, x + 1, x.toString)).toDS

    import simba.simbaImplicits._

    DS1.knnJoin(DS2, Array("x", "y"), Array("x", "y"), 3).show()

    DS1.distanceJoin(DS2, Array("x", "y"), Array("x", "y"), 3).show()

  }

}
