package org.apache.spark.sql.simba.examples

import org.apache.spark.sql.simba.{ Dataset, SimbaSession }
import org.apache.spark.sql.simba.index.{ RTreeType, TreapType }
import org.apache.spark.sql.simba.partitioner.STRPartitioner

object CS236Query {
  case class PointOfInterest(id: Long, desc: String, lat: Double, lon: Double)
  //case class trajectories(id: Long, seqid: Long, lat: Double, lon: Double)
  
  def main(args: Array[String]): Unit = {
    val simbaSession = SimbaSession
      .builder()
      .master("local[4]")
      .appName("CS236BuildingRTreeIndex")
      .config("simba.index.partitions", "64")
      .getOrCreate()
     
    //query A  
    retrievingShopQuery(simbaSession)
    
    //query B
    //objectsNearAirportQuery(simbaSession)
    simbaSession.stop()
  }
  
  private def retrievingShopQuery(simba: SimbaSession): Unit = {
    import simba.implicits._  
    import simba.simbaImplicits._
    val df = simba.read.option("header", false).csv("datasets/POIs.csv")
    val df2 = df.toDF("id", "desc", "lat", "lon")
    val df3 = df2.filter("lat IS NOT NULL").filter("lon IS NOT NULL")
    val df4 = df3.map(row => (row.getString(0).toLong, row.getString(1), 
        row.getString(2).toDouble, row.getString(3).toDouble)).toDF("id", "desc", "lat", "lon")
    df4.index(RTreeType, "testqtree",  Array("lat", "lon"))   
    df4.printSchema()
    //caseClassDS.range(Array("x", "y"),Array(1.0, 1.0),Array(3.0, 3.0)).show()
    val df5=df4.range(Array("lat", "lon"),Array(-332729.310,4456050.000),Array(-316725.862,4469518.966)).filter($"desc".contains("shop"))
    df5.show()
    
  }
  
}