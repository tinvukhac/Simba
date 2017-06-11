package org.apache.spark.sql.simba.examples

import org.apache.spark.sql.simba.Dataset
import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.simba.index.RTreeType
import org.apache.spark.sql.functions.{ unix_timestamp, from_unixtime, hour, minute }

object CS236QueryD {
  
  case class Trajectory(trajId: Long, seqId: Long, lon: Double, lat: Double)
  
  def main(args: Array[String]): Unit = {
    val simbaSession = SimbaSession
      .builder()
      .master("local[4]")
      .appName("CS236BuildingRTreeIndex")
      .config("simba.index.partitions", "64")
      .getOrCreate()

    findTopPolularPoints(simbaSession, "datasets/trajectories.csv", 1000)
    simbaSession.stop()
  }
  
  private def findTopPolularPoints(simba: SimbaSession, dataset: String, radius: Double): Unit = {
    import simba.implicits._
    import simba.simbaImplicits._
    
    // Load data then index by R-Tree
    val weekends = List("Friday", "Saturday")
    val df = simba.read.option("header", false).csv(dataset)
    val df2 = df.toDF("trajId", "seqId", "lon", "lat", "time")
    val df3 = df2.filter("lat IS NOT NULL").filter("lon IS NOT NULL")
    val df4 = df3.select($"trajId", $"seqId", $"lon", $"lat", $"time",
      from_unixtime(unix_timestamp($"time", "yyyy-MM-dd HH:mm:ss"), "EEEEE").alias("dow"))
    val df5 = df4.filter($"dow".isin(weekends:_*))
    val ds = df5.map(row => Trajectory(row.getString(0).toLong, row.getString(1).toLong, row.getString(2).toDouble,
      row.getString(3).toDouble))
    ds.index(RTreeType, "rtreeindex",  Array("lon", "lat"))
    
    // Check circle range query
    val circleRangeDF = ds.circleRange(Array("lon", "lat"), Array(-316991.202308286, 4458298.339010008), radius)
    circleRangeDF.printSchema()
    circleRangeDF.show()
    
    // Execute circle range query for all point of ds
    val countedPoints = ds.map(row => {
      ds.circleRange(Array("lon", "lat"), Array(row.lon, row.lat), radius)
      (row.lon, row.lat, 0)
    })
    countedPoints.printSchema()
    countedPoints.show()
  }
}