package org.apache.spark.sql.simba.examples

import org.apache.spark.sql.simba.Dataset
import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.simba.index.RTreeType
import org.apache.spark.sql.simba.spatial.MBR;
import org.apache.spark.sql.simba.spatial.Point;
import scala.collection._
import java.io._

object CS236BenchmarkQueryC {
  
  case class Trajectory(trajId: Long, seqId: Long, lon: Double, lat: Double)
  case class GridCell(row: Int, column: Int, count: Int)

  def main(args: Array[String]): Unit = {
    for (core <- 1 to 4) {
      val poiFile = new File("query_results/BenchmarkQueryC_" + core + "cores.txt")
      val poiBW = new BufferedWriter(new FileWriter(poiFile))
      
      for (size <- 100 to 1000 by 100) {
        val simbaSession = SimbaSession
        .builder()
        .master("local[" + core + "]")
        .appName("CS236BenchmarkQueryC_" + core + "_" + size)
        .config("simba.index.partitions", "64")
        .getOrCreate()
        
        val start = System.nanoTime()
        CS236QueryC.aggregate(simbaSession, "datasets/trajectories.csv", size, -332729.310, 4456050.000, -316725.862, 4469518.966)
        val end = System.nanoTime()
        println("Time elapsed for  " + (end - start) / 1000 + " microsecs")
        val tep = (end - start) / (1000 * 1000)
        poiBW.write(tep.toString() + '\n')
        
        simbaSession.stop()
      }
      poiBW.close()
    }
  }
}