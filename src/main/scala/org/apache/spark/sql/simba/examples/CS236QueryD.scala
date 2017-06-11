package org.apache.spark.sql.simba.examples

import org.apache.spark.sql.simba.Dataset
import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.simba.index.RTreeType
import org.apache.spark.sql.functions.{ unix_timestamp, from_unixtime, hour, minute }
import java.io._

object CS236QueryD {

  case class Trajectory(trajId: Long, seqId: Long, lon: Double, lat: Double)

  def main(args: Array[String]): Unit = {
    val simbaSession = SimbaSession
      .builder()
      .master("local[4]")
      .appName("CS236BuildingRTreeIndex")
      .config("simba.index.partitions", "64")
      .getOrCreate()

    findTopPolularPoints(simbaSession, "datasets/trajectories.csv", 10)
    simbaSession.stop()
  }

  private def findTopPolularPoints(simba: SimbaSession, dataset: String, radius: Double): Unit = {
    import simba.implicits._
    import simba.simbaImplicits._

    // Load data then index by R-Tree
    val weekends = List("Friday", "Saturday")
    val unix = unix_timestamp($"time", "yyyy-MM-dd HH:mm:ss")
    val df = simba.read.option("header", false).csv(dataset)
    val df2 = df.toDF("trajId", "seqId", "lon", "lat", "time")
    val df3 = df2.filter("lat IS NOT NULL").filter("lon IS NOT NULL")
    val df4 = df3.select($"trajId", $"seqId", $"lon", $"lat", $"time",
      from_unixtime(unix, "EEEEE").alias("dow"), from_unixtime(unix, "yyyy").alias("year"))
    val df5 = df4.filter($"year".contains("2009")).filter($"dow".isin(weekends: _*))
//    val ds = df5.map(row => Trajectory(row.getString(0).toLong, row.getString(1).toLong, row.getString(2).toDouble,
//      row.getString(3).toDouble))
    //    ds.index(RTreeType, "rtreeindex",  Array("lon", "lat"))

    // Find the MBR of filtered dataset
    var minLat = Double.MaxValue
    var minLon = Double.MaxValue
    var maxLat = Double.MinValue
    var maxLon = Double.MinValue
    val mbrs = df5.collect().foreach(row => {
      val lon = row.getString(2).toDouble
      val lat = row.getString(3).toDouble
      if (minLat > lat) minLat = lat
      if (maxLat < lat) maxLat = lat
      if (minLon > lon) minLon = lon
      if (maxLon < lon) maxLon = lon
    })

    // Compute latitude and longtitude offset corresponding to cell size 
    val height = haversineDistance((minLon / 1000000, minLat / 1000000), (maxLon / 1000000, minLat / 1000000))
    val width = haversineDistance((minLon / 1000000, minLat / 1000000), (minLon / 1000000, maxLat / 1000000))
    val lonOffset = (maxLon - minLon) / height * radius
    val latOffset = (maxLat - minLat) / width * radius

    // Group points with same cell together
    df5.printSchema()
    val groups = df5.map(row => {
      val lon = row.getString(2).toDouble
      val lat = row.getString(3).toDouble
      val cellRow = math.floor((lon - minLon) / lonOffset)
      val cellColumn = math.floor((lat - minLat) / latOffset)
      (cellRow, cellColumn, 1)
    }).groupBy($"_1", $"_2").count()

    // Compute center point from cell row and column
    val results = groups.map(row => {
      val cellRow = row.getDouble(0)
      val cellColumn = row.getDouble(1)
      val cellCenterLon = minLon + (cellRow + 0.5) * lonOffset
      val cellCenterLat = minLat + (cellColumn + 0.5) * latOffset
      (cellCenterLon, cellCenterLat, row.getLong(2))
    }).orderBy($"_3".desc).limit(10)
    
    results.show()

    // Write points to file
    //    val pointFile = new File("mbr_plot/sample_points.txt")
    //    val pointBW = new BufferedWriter(new FileWriter(pointFile))
    //    ds.collect().foreach(poi => pointBW.write(poi.lat + "," + poi.lon + "\n"))
    //    pointBW.close()
  }

  // Compute distance between 2 points
  def haversineDistance(pointA: (Double, Double), pointB: (Double, Double)): Double = {
    val deltaLat = math.toRadians(pointB._1 - pointA._1)
    val deltaLong = math.toRadians(pointB._2 - pointA._2)
    val a = math.pow(math.sin(deltaLat / 2), 2) + math.cos(math.toRadians(pointA._1)) * math.cos(math.toRadians(pointB._1)) * math.pow(math.sin(deltaLong / 2), 2)
    val greatCircleDistance = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    6371008.263 * greatCircleDistance
  }
}