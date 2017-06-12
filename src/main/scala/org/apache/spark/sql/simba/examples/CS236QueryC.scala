package org.apache.spark.sql.simba.examples

import org.apache.spark.sql.simba.Dataset
import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.simba.index.RTreeType
import org.apache.spark.sql.simba.spatial.MBR;
import org.apache.spark.sql.simba.spatial.Point;
import scala.collection._
import java.io._

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
      
      // FileWriter
      val file = new File("QcCore1.txt")
      val bw = new BufferedWriter(new FileWriter(file))
        //bw.write(text)
    for (i <- 100 to 1000)
    {  
      val start = System.nanoTime() 
      aggregate(simbaSession, "datasets/trajectories.csv", i, -332729.310, 4456050.000, -316725.862, 4469518.966)
      val end = System.nanoTime()
      //println((end-start)/1000 )
      //bw.write()
      
    }
    
  }

  private def aggregate(simba: SimbaSession, dataset: String, cellSize: Double,
    lon1: Double, lat1: Double, lon2: Double, lat2: Double): Unit = {
    import simba.implicits._
    import simba.simbaImplicits._
    
    // Load data then index by R-Tree
    val df = simba.read.option("header", false).csv(dataset)
    val df2 = df.toDF("trajId", "seqId", "lon", "lat", "time")
    val df3 = df2.filter("lat IS NOT NULL").filter("lon IS NOT NULL")
    val ds = df3.map(row => Trajectory(row.getString(0).toLong, row.getString(1).toLong, row.getString(2).toDouble,
      row.getString(3).toDouble))
//    ds.index(RTreeType, "rtreeindex",  Array("lon", "lat"))

    // Extract all the trajectory points inside the 3rd road ring uing range query
    val rangeDF = ds.range(Array("lon", "lat"), Array(lon1, lat1), Array(lon2, lat2))

    // Compute latitude and longtitude offset corresponding to cell size 
    val height = haversineDistance((lon1 / 1000000, lat1 / 1000000), (lon2 / 1000000, lat1 / 1000000))
    val width = haversineDistance((lon1 / 1000000, lat1 / 1000000), (lon1 / 1000000, lat2 / 1000000))
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
    val pointFile = new File("mbr_plot/center_points.txt")
    val pointBW = new BufferedWriter(new FileWriter(pointFile))
    results.collect().foreach(row => pointBW.write(row._1 + "," + row._2 + "," + row._3 + "\n"))
    pointBW.close()
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