package org.apache.spark.sql.simba.examples

import org.apache.spark.sql.simba.Dataset
import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.simba.index.RTreeType
import org.apache.spark.sql.functions.{ unix_timestamp, from_unixtime, hour, minute }
import java.io._

/**
 * Find top-10 more popular POIs in 2009
 */
object CS236QueryD {

  case class PointOfInterest(id: Long, desc: String, poiLon: Double, poiLat: Double)
  case class Trajectory(trajId: Long, seqId: Long, trajLon: Double, trajLat: Double)

  def main(args: Array[String]): Unit = {
    val simbaSession = SimbaSession
      .builder()
      .master("local[4]")
      .appName("CS236BuildingRTreeIndex")
      .config("simba.index.partitions", "64")
      .getOrCreate()

    findTopPolularPoints(simbaSession, "datasets/POIs.csv", "datasets/trajectories.csv", 100)
    simbaSession.stop()
  }

  def findTopPolularPoints(simba: SimbaSession, poiDataset: String, trajectoryDataset: String, radius: Double): Unit = {
    import simba.implicits._
    import simba.simbaImplicits._
    
    // Read data from POI dataset
    val poiDF = simba.read.option("header", false).csv(poiDataset)
    .toDF("id", "desc", "lon", "lat")
    .filter("lon IS NOT NULL")
    .filter("lat IS NOT NULL")
    val poiDS = poiDF.map(row => PointOfInterest(row.getString(0).toLong, row.getString(1), 
        row.getString(2).toDouble, row.getString(3).toDouble))
    poiDS.index(RTreeType, "poirtreeindex",  Array("poiLon", "poiLat"))

    // Read data from trajectories dataset
    val weekends = List("Friday", "Saturday")
    val unix = unix_timestamp($"time", "yyyy-MM-dd HH:mm:ss")
    val df = simba.read.option("header", false).csv(trajectoryDataset)
    val df2 = df.toDF("trajId", "seqId", "lon", "lat", "time")
    val df3 = df2.filter("lon IS NOT NULL").filter("lat IS NOT NULL")
    val df4 = df3.select($"trajId", $"seqId", $"lon", $"lat", $"time",
      from_unixtime(unix, "EEEEE").alias("dow"), from_unixtime(unix, "yyyy").alias("year"))
    // Filter data of weekends in 2009 
    val df5 = df4.filter($"year".contains("2009")).filter($"dow".isin(weekends: _*))
    val trajectoryDS = df5.map(row => Trajectory(row.getString(0).toLong, row.getString(1).toLong, row.getString(2).toDouble,
      row.getString(3).toDouble))
    trajectoryDS.index(RTreeType, "trajrtreeindex",  Array("trajLon", "trajLat"))
    
    // Compute distance join between POIs and trajectories
    val distanceJoinResults = poiDS.distanceJoin(trajectoryDS, Array("poiLon", "poiLat"), Array("trajLon", "trajLat"), radius)
    .groupBy("id").count().orderBy($"count".desc).limit(10)
    distanceJoinResults.printSchema()
    distanceJoinResults.show()
    
    // Write results to file
    val pointFile = new File("query_results/query_D.csv")
    val pointBW = new BufferedWriter(new FileWriter(pointFile))
    distanceJoinResults.collect().foreach(row => pointBW.write(row.getLong(0) + "," + row.getLong(1) + "\n"))
    pointBW.close()
  }
}