package org.apache.spark.sql.simba.examples

import org.apache.spark.sql.simba.Dataset
import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.simba.index.RTreeType
import org.apache.spark.sql.simba.spatial.MBR;
import org.apache.spark.sql.simba.spatial.Point;
import scala.collection._
import java.io._

/**
 * Point aggregation
 */
object CS236QueryC {

  case class Trajectory(trajId: Long, seqId: Long, lon: Double, lat: Double)
  case class GridCell(row: Int, column: Int, count: Int)

  def main(args: Array[String]): Unit = {
    val simbaSession = SimbaSession
      .builder()
      .master("local[1]")
      .appName("CS236BuildingRTreeIndex")
      .config("simba.index.partitions", "64")
      .getOrCreate()
      
    aggregate(simbaSession, "datasets/trajectories.csv", 500, -332729.310, 4456050.000, -316725.862, 4469518.966)
    simbaSession.stop()
    
  }

  def aggregate(simba: SimbaSession, dataset: String, cellSize: Double,
    lon1: Double, lat1: Double, lon2: Double, lat2: Double): Unit = {
    import simba.implicits._
    import simba.simbaImplicits._
    
    // Read data from dataset, notice that we need to remove missing data points
    val df = simba.read.option("header", false).csv(dataset)
    .toDF("trajId", "seqId", "lon", "lat", "time")
    .filter("lat IS NOT NULL")
    .filter("lon IS NOT NULL")
    val ds = df.map(row => Trajectory(row.getString(0).toLong, row.getString(1).toLong, row.getString(2).toDouble,
      row.getString(3).toDouble))

    // Extract all the trajectory points inside the 3rd road ring using range query
    val rangeDF = ds.range(Array("lon", "lat"), Array(lon1, lat1), Array(lon2, lat2))

    // Compute latitude and longtitude offset corresponding to cell size 
    val M = 1000000
    val height = haversineDistance((lat1 / M, lon1 / M), (lat1 / M, lon2 / M))
    val width = haversineDistance((lat1 / M, lon1 / M), (lat2 / M, lon1 / M))
    val lonOffset = (lon2 - lon1) / height * cellSize
    val latOffset = (lat2 - lat1) / width * cellSize

    // Group points with same cell together
    val groups = rangeDF.map(row => {
      val lon = row.getDouble(2)
      val lat = row.getDouble(3)
      val cellRow = math.floor((lon - lon1) / lonOffset)
      val cellColumn = math.floor((lat - lat1) / latOffset)
      (cellRow, cellColumn, 1)
    }).groupBy($"_1", $"_2").count()

    // Compute center point from cell row and column
    val results = groups.map(row => {
      val cellRow = row.getDouble(0)
      val cellColumn = row.getDouble(1)
      val cellCenterLon = lon1 + (cellRow + 0.5) * lonOffset
      val cellCenterLat = lat1 + (cellColumn + 0.5) * latOffset
      (cellCenterLon, cellCenterLat, row.getLong(2))
    })

    // Write results to file
    val pointFile = new File("query_results/center_points.csv")
    val pointBW = new BufferedWriter(new FileWriter(pointFile))
    results.collect().foreach(row => pointBW.write(row._1 + "," + row._2 + "," + row._3 + "\n"))
    pointBW.close()
  }

  // Compute Haversine distance between 2 points
  def haversineDistance(pointA: (Double, Double), pointB: (Double, Double)): Double = {
    val deltaLat = math.toRadians(pointB._1 - pointA._1)
    val deltaLong = math.toRadians(pointB._2 - pointA._2)
    val a = math.pow(math.sin(deltaLat / 2), 2) + math.cos(math.toRadians(pointA._1)) * math.cos(math.toRadians(pointB._1)) * math.pow(math.sin(deltaLong / 2), 2)
    val greatCircleDistance = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    6371008.263 * greatCircleDistance
  }
}