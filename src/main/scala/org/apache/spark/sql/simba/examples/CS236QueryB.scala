package org.apache.spark.sql.simba.examples

import java.sql.Date
import org.apache.spark.sql.simba.{ Dataset, SimbaSession }
import org.apache.spark.sql.simba.index.{ RTreeType, TreapType }
import org.apache.spark.sql.simba.partitioner.STRPartitioner
import org.apache.spark.sql.functions.unix_timestamp


object CS236QueryB {
  case class trajectories(id: Long, seqid: Long, lat: Double, lon: Double, tdate:Date)
  def main(args: Array[String]): Unit = {
    val simbaSession = SimbaSession
      .builder()
      .master("local[4]")
      .appName("CS236BuildingRTreeIndex")
      .config("simba.index.partitions", "64")
      .getOrCreate()
     objectsNearAirportQuery(simbaSession)
     simbaSession.stop()
  }
 
  private def objectsNearAirportQuery(simba: SimbaSession): Unit = {
    
    import simba.implicits._  
    import simba.simbaImplicits._
    val df = simba.read.option("header", false).csv("datasets/trajectories.csv")
    val df2 = df.toDF("id", "seqid", "lat", "lon","tdate")
    val df3 = df2.filter("lat IS NOT NULL").filter("lon IS NOT NULL")
    val df4 = df3.map(row => (row.getString(0).toLong, row.getString(1).toLong, 
       row.getString(2).toDouble, row.getString(3).toDouble,unix_timestamp($"tdate","MM/dd/yyyy HH:mm:ss"))).toDF("id", "seqid", "lat", "lon","tdate")
    df4.printSchema()
    df4.show()
  }
   
}