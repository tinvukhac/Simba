package org.apache.spark.sql.simba.examples

import org.apache.spark.sql.simba.Dataset
import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.simba.index.RTreeType
import org.apache.spark.sql.functions.{ unix_timestamp, from_unixtime, hour, minute, to_date }
import java.sql.Timestamp
import java.text.SimpleDateFormat
import org.apache.spark.sql.functions._

/**
 * How many objects (in average) are around 5 Km of the Beijing Capital
 * International Airport (-302967.241,4481467.241) on Friday nights (between 8:00PM and 11:59PM)
 */
object CS236QueryB {
  case class Trajectory(trajId: Long, seqId: Long, lon: Double, lat: Double, time: Timestamp, dow: String, hour: Integer, date: String)
  def main(args: Array[String]): Unit = {
    val simbaSession = SimbaSession
      .builder()
      .master("local[4]")
      .appName("CS236BuildingRTreeIndex")
      .config("simba.index.partitions", "64")
      .getOrCreate()

    findNearbyObjects(simbaSession, "datasets/trajectories.csv", -302967.241, 4481467.241, 5000)

    simbaSession.stop()
  }

  private def findNearbyObjects(simba: SimbaSession, dataset: String, lon: Double, lat: Double, r: Double): Unit = {

    val simba = SimbaSession
      .builder()
      .master("local[4]")
      .appName("CS236BuildingRTreeIndex")
      .config("simba.index.partitions", "64")
      .getOrCreate()

    import simba.implicits._
    import simba.simbaImplicits._

    val hours = List(20, 21, 22, 23) // Time range (8-11 PM)
    val unix = unix_timestamp($"time", "yyyy-MM-dd HH:mm:ss")
    val datex = unix_timestamp($"time", "yyyy-MM-dd")

    // Read data from trajectories dataset
    val df = simba.read.option("header", false).csv(dataset)
    val df2 = df.toDF("trajId", "seqId", "lon", "lat", "time")
    val df3 = df2.filter("lon IS NOT NULL").filter("lat IS NOT NULL")

    // Add new column for filter process: dow (day of week) and hour of day 
    val df4 = df3.select($"trajId", $"seqId", $"lon", $"lat", unix.cast("timestamp").alias("timestamp"),
      from_unixtime(unix, "EEEEE").alias("dow"), hour(unix.cast("timestamp")).alias("hour"), from_unixtime(unix, "yyyy-MM-dd").alias("date"))
    val df5 = df4.filter($"dow".contains("Friday")).filter($"hour".isin(hours: _*));
    val ds = df5.map(row => Trajectory(row.getString(0).toLong, row.getString(1).toLong, row.getString(2).toDouble,
      row.getString(3).toDouble, row.getTimestamp(4), row.getString(5), row.getInt(6), row.getString(7)))

    // Compute the average 
    val df9 = ds.circleRange(Array("lon", "lat"), Array(lon, lat), r).groupBy("date").count();
    df9.show()
    df9.select(avg($"count")).show()
    simba.stop()
  }

}