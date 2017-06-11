package org.apache.spark.sql.simba.examples

import org.apache.spark.sql.simba.Dataset
import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.simba.index.{ RTreeType, TreapType }
import org.apache.spark.sql.simba.partitioner.STRPartitioner
import org.apache.spark.sql.simba.spatial.MBR
import org.apache.spark.sql.simba.spatial.Point
import java.io._

/*
 * Read 10% of POI dataset then create a R-Tree index
 */

object CS236BuildingRTreeIndex {

  case class PointOfInterest(id: Long, desc: String, lat: Double, lon: Double)
  case class PartitionMBR(x1: Double, y1: Double, x2: Double, y2: Double)

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
    import simba.simbaImplicits._
    val df = simba.read.option("header", false).csv("datasets/POIs.csv").limit(6000)
    val df2 = df.toDF("id", "desc", "lat", "lon")
    val df3 = df2.filter("lat IS NOT NULL").filter("lon IS NOT NULL")
    val ds = df3.map(row => PointOfInterest(row.getString(0).toLong, row.getString(1), 
        row.getString(3).toDouble, row.getString(2).toDouble))
    ds.index(RTreeType, "rtreeindex",  Array("lat", "lon"))
    
    val mbrs = ds.mapPartitions(iter => {
        var minX = Double.MaxValue
        var minY = Double.MaxValue
        var maxX = Double.MinValue
        var maxY = Double.MinValue
        
        while(iter.hasNext) {
          val poi = iter.next()
          if(minX > poi.lat) minX = poi.lat
          if(maxX < poi.lat) maxX = poi.lat
          if(minY > poi.lon) minY = poi.lon
          if(maxY < poi.lon) maxY = poi.lon
        }
        val mbr = List(PartitionMBR(minX, minY, maxX, maxY))
        mbr.iterator
      }
    ).collect()
    println(mbrs.mkString("\n"))
    
    val mbrFile = new File("mbrs.txt")
    val mbrBW = new BufferedWriter(new FileWriter(mbrFile))
    mbrs.foreach(mbr => mbrBW.write(mbr.x1 + "," + mbr.y1 + "," + mbr.x2 + "," + mbr.y2 + "\n"))
    mbrBW.close()
    
    val pointFile = new File("points.txt")
    val pointBW = new BufferedWriter(new FileWriter(pointFile))
    ds.collect().foreach(poi => pointBW.write(poi.lat + "," + poi.lon + "\n"))
    pointBW.close()
  }
}