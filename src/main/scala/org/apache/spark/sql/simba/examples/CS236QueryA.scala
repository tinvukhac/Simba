package org.apache.spark.sql.simba.examples

import org.apache.spark.sql.simba.{ Dataset, SimbaSession }
import org.apache.spark.sql.simba.index.{ RTreeType, TreapType }
import org.apache.spark.sql.simba.partitioner.STRPartitioner
import java.io._

object CS236QueryA {

  case class PointOfInterest(id: Long, desc: String, lon: Double, lat: Double)

  def main(args: Array[String]): Unit = {
    val simbaSession = SimbaSession
      .builder()
      .master("local[4]")
      .appName("CS236BuildingRTreeIndex")
      .config("simba.index.partitions", "64")
      .getOrCreate()

    retrievingShopQuery(simbaSession)
    simbaSession.stop()
  }

  private def retrievingShopQuery(simba: SimbaSession): Unit = {
    import simba.implicits._
    import simba.simbaImplicits._
    // Read data from POI dataset
    val df = simba.read.option("header", false).csv("datasets/POIs.csv")
      .toDF("id", "desc", "lon", "lat")
      .filter("lon IS NOT NULL")
      .filter("lat IS NOT NULL")
    val df2 = df.map(row => (row.getString(0).toLong, row.getString(1),
      row.getString(2).toDouble, row.getString(3).toDouble)).toDF("id", "desc", "lon", "lat")

//    df2.index(RTreeType, "queryartreeindex", Array("lon", "lat"))

    // Execute range query then filter by shop
    val range = df2.range(Array("lon", "lat"), Array(-332729.310, 4456050.000), Array(-316725.862, 4469518.966))
    .filter($"desc".contains("shop="))
    range.show()
    
    // Write result to files 
    val poiFile = new File("query_results/query_A.csv")
    val poiBW = new BufferedWriter(new FileWriter(poiFile))
    range.collect().foreach(poi => poiBW.write(poi.getLong(0) + ",\"" + poi.getString(1) + "\"," + poi.getDouble(2) + "," + poi.getDouble(3) + "\n"))
    poiBW.close()
  }

}