package org.apache.spark.sql.simba.examples

import org.apache.spark.sql.simba.Dataset
import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.simba.index.RTreeType
import org.apache.spark.sql.functions.{ unix_timestamp, from_unixtime, hour, minute }
import java.sql.Timestamp
import java.text.SimpleDateFormat

object Hello {

  case class Trajectory(trajId: Long, seqId: Long, lon: Double, lat: Double, time: Timestamp, dow: String, hour: Integer)

  def main(args: Array[String]): Unit = {
    val simba = SimbaSession
      .builder()
      .master("local[4]")
      .appName("CS236BuildingRTreeIndex")
      .config("simba.index.partitions", "64")
      .getOrCreate()

    import simba.implicits._
    import simba.simbaImplicits._
    val hours = List(20, 21, 22, 23)
    val unix = unix_timestamp($"time", "yyyy-MM-dd HH:mm:ss")
    val df = simba.read.option("header", false).csv("datasets/trajectories.csv")
    val df2 = df.toDF("trajId", "seqId", "lon", "lat", "time")
    val df3 = df2.filter("lat IS NOT NULL").filter("lon IS NOT NULL")
    val df4 = df3.select($"trajId", $"seqId", $"lon", $"lat", unix.cast("timestamp").alias("timestamp"),
      from_unixtime(unix, "EEEEE").alias("dow"), hour(unix.cast("timestamp")).alias("hour"))
    val df5 = df4.filter($"dow".contains("Friday")).filter($"hour".isin(hours:_*));
    val ds = df5.map(row => Trajectory(row.getString(0).toLong, row.getString(1).toLong, row.getString(2).toDouble, 
        row.getString(3).toDouble, row.getTimestamp(4), row.getString(5), row.getInt(6)))
    ds.printSchema()
    ds.show()

    simba.stop()
  }
}