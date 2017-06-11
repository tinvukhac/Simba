package org.apache.spark.sql.simba.examples

import org.apache.spark.sql.simba.Dataset
import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.simba.index.RTreeType
import org.apache.spark.sql.functions.{unix_timestamp, to_date}
import java.sql.Date
import java.text.SimpleDateFormat

object Hello {
  
  case class Trajectory(trajId: Long, seqId: Long, lon: Double, lat:Double, time: Date)
  
 def main(args: Array[String]): Unit = {
   val simba = SimbaSession
      .builder()
      .master("local[4]")
      .appName("CS236BuildingRTreeIndex")
      .config("simba.index.partitions", "64")
      .getOrCreate()
      
    import simba.implicits._  
    import simba.simbaImplicits._
    val df = simba.read.option("header", false).csv("datasets/trajectories.csv")
    val df2 = df.toDF("trajId", "seqId", "lon", "lat", "time")
    val df3 = df2.filter("lat IS NOT NULL").filter("lon IS NOT NULL") 
    val df4 = df3.select($"trajId", $"seqId", $"lon", $"lat", to_date(unix_timestamp($"time", "yyyy-MM-dd HH:mm:ss").cast("timestamp")).alias("timestamp"))
    val ds = df4.map(row => Trajectory(row.getString(0).toLong, row.getString(1).toLong, 
        row.getString(2).toDouble, row.getString(3).toDouble, row.getDate(4)))
    ds.printSchema()
    ds.show()
//    val ds2 = ds.select(to_date(unix_timestamp($"time", "yyyy-MM-dd hh:MM:ss").cast("timestamp")).alias("timestamp"))
//    ds2.printSchema()
//    ds2.show()
    df4.printSchema()
    df4.show()
    
    simba.stop()
  }
}