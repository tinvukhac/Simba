package org.apache.spark.sql.simba.examples

import org.apache.spark.sql.simba.Dataset
import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.simba.index.RTreeType
import org.apache.spark.sql.functions.{ unix_timestamp, from_unixtime, hour, minute }
import java.io._

object CS236QueryD {

  case class Trajectory1(trajId: Long, seqId: Long, lon1: Double, lat1: Double)
  case class Trajectory2(trajId: Long, seqId: Long, lon2: Double, lat2: Double)

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
    val ds = df5.map(row => Trajectory1(row.getString(0).toLong, row.getString(1).toLong, row.getString(2).toDouble,
      row.getString(3).toDouble))
    val ds2 = df5.map(row => Trajectory2(row.getString(0).toLong, row.getString(1).toLong, row.getString(2).toDouble,
      row.getString(3).toDouble))
    ds.index(RTreeType, "rtreeindex",  Array("lon1", "lat1"))
    ds2.index(RTreeType, "rtreeindex",  Array("lon2", "lat2"))
    
    val distanceJoinResults = ds.distanceJoin(ds2, Array("lon1", "lat1"), Array("lon2", "lat2"), radius)
    .groupBy("lon1", "lat1").count().orderBy($"count".desc).limit(10)
    distanceJoinResults.printSchema()
    distanceJoinResults.show()
  }
}