package org.apache.spark.sql.simba.examples

import org.apache.spark.sql.simba.{ Dataset, SimbaSession }
import org.apache.spark.sql.simba.index.{ RTreeType, TreapType }
import org.apache.spark.sql.simba.partitioner.STRPartitioner

/*
 * Read 10% of POI dataset then create a R-Tree index
 */

object CS236BuildingRTreeIndex {

  case class PointOfInterest(id: Long, desc: String, lat: Double, lon: Double)

  def main(args: Array[String]): Unit = {
    val simbaSession = SimbaSession
      .builder()
      .master("local[4]")
      .appName("CS236BuildingRTreeIndex")
      .config("simba.index.partitions", "64")
      .getOrCreate()

    buildRTreeIndex(simbaSession)
    simbaSession.stop()
  }
  
  private def buildRTreeIndex(simba: SimbaSession): Unit = {
    import simba.implicits._
    val df = simba.read.option("header", false).csv("datasets/POIs.csv").limit(6000)
    val df2 = df.toDF("id", "desc", "lat", "lon")
    val df3 = df2.filter("lat IS NOT NULL").filter("lon IS NOT NULL")
    val df4 = df3.map(row => (row.getString(0).toLong, row.getString(1), 
        row.getString(2).toDouble, row.getString(3).toDouble)).toDF("id", "desc", "lat", "lon")
    df4.createOrReplaceTempView("a")
    simba.indexTable("a", RTreeType, "testqtree",  Array("lat", "lon") )
    simba.showIndex("a")
  }
}