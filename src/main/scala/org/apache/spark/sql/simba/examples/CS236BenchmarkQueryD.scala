package org.apache.spark.sql.simba.examples

import org.apache.spark.sql.simba.Dataset
import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.simba.index.RTreeType
import org.apache.spark.sql.functions.{ unix_timestamp, from_unixtime, hour, minute }
import java.io._

object CS236BenchmarkQueryD {

  case class PointOfInterest(id: Long, desc: String, poiLon: Double, poiLat: Double)
  case class Trajectory(trajId: Long, seqId: Long, trajLon: Double, trajLat: Double)

  def main(args: Array[String]): Unit = {
    for (core <- 1 to 4) {
      val poiFile = new File("benchmark_results/BenchmarkQueryD_" + core + "cores.txt")
      val poiBW = new BufferedWriter(new FileWriter(poiFile))

      for (radius <- 10 to 100 by 10) {
        val simbaSession = SimbaSession
        .builder()
        .master("local[" + core + "]")
        .appName("CS236BenchmarkQueryD_" + core + "_" + radius)
        .config("simba.index.partitions", "64")
        .getOrCreate()
        
        val start = System.nanoTime()
        CS236QueryD.findTopPolularPoints(simbaSession, "datasets/POIs.csv", "datasets/trajectories.csv", radius)
        val end = System.nanoTime()
        println("Time elapsed for  " + (end - start) / 1000 + " microsecs")
        val tep = (end - start) / 1000
        poiBW.write(tep.toString() + '\n')
        
        simbaSession.stop()
      }
      poiBW.close()
    }
  }
}