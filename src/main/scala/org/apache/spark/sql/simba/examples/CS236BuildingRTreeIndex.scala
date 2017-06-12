package org.apache.spark.sql.simba.examples

import org.apache.spark.sql.simba.Dataset
import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.simba.index.RTreeType
import java.io._

/*
 * Read 10% of POI dataset then create a R-Tree index
 */

object CS236BuildingRTreeIndex {

  case class PointOfInterest(id: Long, desc: String, lon: Double, lat: Double)
  case class PartitionMBR(x1: Double, y1: Double, x2: Double, y2: Double)

  def main(args: Array[String]): Unit = {
    val simbaSession = SimbaSession
      .builder()
      .master("local[4]")
      .appName("CS236BuildingRTreeIndex")
      .config("simba.index.partitions", "64")
      .getOrCreate()

    buildRTreeIndex(simbaSession, "datasets/POIs.csv", 0.1)
    simbaSession.stop()
  }
  
  private def buildRTreeIndex(simba: SimbaSession, dataset: String, fraction: Double): Unit = {
    import simba.implicits._  
    import simba.simbaImplicits._
    
    // Read sample data from POI dataset
    val df = simba.read.option("header", false).csv(dataset).sample(true, fraction)
    .toDF("id", "desc", "lon", "lat")
    .filter("lon IS NOT NULL")
    .filter("lat IS NOT NULL")
    val ds = df.map(row => PointOfInterest(row.getString(0).toLong, row.getString(1), 
        row.getString(2).toDouble, row.getString(3).toDouble))
    ds.printSchema()
    ds.show()
    
    // Index sample data using R-Tree
    ds.index(RTreeType, "rtreeindex",  Array("lon", "lat"))
    
    // Compute MBRs of partitioned data
    val mbrs = ds.mapPartitions(iter => {
        var minLat = Double.MaxValue
        var minLon = Double.MaxValue
        var maxLat = Double.MinValue
        var maxLon = Double.MinValue
        
        while(iter.hasNext) {
          val poi = iter.next()
          if(minLat > poi.lat) minLat = poi.lat
          if(maxLat < poi.lat) maxLat = poi.lat
          if(minLon > poi.lon) minLon = poi.lon
          if(maxLon < poi.lon) maxLon = poi.lon
        }
        List(PartitionMBR(minLat, minLon, maxLat, maxLon)).iterator
      }
    ).collect()
    
    // Write MBRs to file
    val mbrFile = new File("mbr_plot/mbrs.txt")
    val mbrBW = new BufferedWriter(new FileWriter(mbrFile))
    mbrs.foreach(mbr => mbrBW.write(mbr.x1 + "," + mbr.y1 + "," + mbr.x2 + "," + mbr.y2 + "\n"))
    mbrBW.close()
    
    // Write points to file
    val pointFile = new File("mbr_plot/points.txt")
    val pointBW = new BufferedWriter(new FileWriter(pointFile))
    ds.collect().foreach(poi => pointBW.write(poi.lat + "," + poi.lon + "\n"))
    pointBW.close()
  }
}