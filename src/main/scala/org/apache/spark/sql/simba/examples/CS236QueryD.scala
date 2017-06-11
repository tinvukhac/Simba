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
      from_unixtime(unix, "EEEEE").alias("dow"), from_unixtime(unix,"yyyy").alias("year"))
    val df5 = df4.filter($"year".contains("2009")).filter($"dow".isin(weekends:_*))
    val ds = df5.map(row => Trajectory(row.getString(0).toLong, row.getString(1).toLong, row.getString(2).toDouble,
      row.getString(3).toDouble))
    ds.index(RTreeType, "rtreeindex",  Array("lon", "lat"))
    
    // Check circle range query
//    val circleRangeDF = ds.circleRange(Array("lon", "lat"), Array(-316991.202308286, 4458298.339010008), radius)
//    circleRangeDF.printSchema()
//    circleRangeDF.show()
//    println(ds.count())
    
    // Execute circle range query for all point of ds
    ds.printSchema()
    ds.show()
    val countedPoints = df5.map(row => {
      var count = 0
      val lon = row.getString(2).toDouble
      val lat = row.getString(3).toDouble
      val circleRange = ds.circleRange(Array("lon", "lat"), Array(lon, lat), radius)
      count = circleRange.count().toInt
      (lon, lat, count)
    })
//    countedPoints.printSchema()
    countedPoints.show()
    
//    df5.printSchema()
//    df5.show()
//    println(ds.count())
//    println(df.count())
  }
}