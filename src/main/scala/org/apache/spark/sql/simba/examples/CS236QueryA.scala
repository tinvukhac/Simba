package org.apache.spark.sql.simba.examples

import org.apache.spark.sql.simba.{ Dataset, SimbaSession }
import org.apache.spark.sql.simba.index.{ RTreeType, TreapType }
import org.apache.spark.sql.simba.partitioner.STRPartitioner
import java.io._

/**
 * Retrieve all the shops located inside the 3rd road ring of the city.
 * The coordinates which you should use are (-332729.310,4456050.000) and (-316725.862,4469518.966)
 */
object CS236QueryA {

  case class PointOfInterest(id: Long, desc: String, lon: Double, lat: Double)

  def main(args: Array[String]): Unit = {
    val simbaSession = SimbaSession
      .builder()
      .master("local[4]")
      .appName("CS236BuildingRTreeIndex")
      .config("simba.index.partitions", "64")
      .getOrCreate()

    retrievingShopQuery(simbaSession, "datasets/POIs.csv", -332729.310, 4456050.000, -316725.862, 4469518.966, "shop=")
    simbaSession.stop()
  }

  /**
   * Retrieve all the shops located inside a range
   * @param simba
   */
  private def retrievingShopQuery(simba: SimbaSession, dataset: String, lon1: Double, lat1: Double, lon2: Double, lat2: Double, searchKey: String): Unit = {
    import simba.implicits._
    import simba.simbaImplicits._
    // Read data from POI dataset
    val df = simba.read.option("header", false).csv(dataset)
      .toDF("id", "desc", "lon", "lat")
      .filter("lon IS NOT NULL")
      .filter("lat IS NOT NULL")
    val df2 = df.map(row => (row.getString(0).toLong, row.getString(1),
      row.getString(2).toDouble, row.getString(3).toDouble)).toDF("id", "desc", "lon", "lat")

    // Execute range query then filter by search key
    val range = df2.range(Array("lon", "lat"), Array(lon1, lat1), Array(lon2, lat2))
      .filter($"desc".contains(searchKey))
    range.show()

    // Write result to files 
    val poiFile = new File("query_results/query_A.csv")
    val poiBW = new BufferedWriter(new FileWriter(poiFile))
    range.collect().foreach(poi => poiBW.write(poi.getLong(0) + ",\"" + poi.getString(1) + "\"," + poi.getDouble(2) + "," + poi.getDouble(3) + "\n"))
    poiBW.close()
  }

}