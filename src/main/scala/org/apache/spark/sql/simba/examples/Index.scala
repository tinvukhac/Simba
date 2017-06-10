package org.apache.spark.sql.simba.examples
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.simba.index.RTreeType

object Index {
  case class POI(pid: Long, poi_lon: Double, poi_lat: Double, tags: String)
  case class Car(cid:Long, car_lon: Double, car_lat: Double)
  case class POI_old(pid: Long,tags: String, poi_lon: Double, poi_lat: Double)
  case class Traj(id:Long, seqid:Long,traj_lon: Double, traj_lat: Double)
  
  def main(args: Array[String]): Unit = {
    val simba = SimbaSession.
                builder().
                master("local[4]").
                appName("Demo").
                config("simba.index.partitions", "8").
                getOrCreate()
                
     import simba.implicits._  
     import simba.simbaImplicits._
     simba.sparkContext.setLogLevel("ERROR")
     
     var schema = ScalaReflection.schemaFor[POI].dataType.asInstanceOf[StructType]
     val poisDS = simba.read.
                  option("header", "true").
                  schema(schema).
                  csv("POIs.csv").
                  as[POI]

    poisDS.show(5)
    //poisDS.createTempView("po")
    //simba.indexTable("po", RTreeType, "RtreeForData",  Array("poi_lon", "poi_lat") )
    //simba.showIndex("po")
    poisDS.index(RTreeType, "poisRT", Array("poi_lon", "poi_lat"))
    poisDS.printSchema
   
    schema = ScalaReflection.schemaFor[Car].dataType.asInstanceOf[StructType]
    val carsDS = simba.read.
                  option("header", "true").
                  schema(schema).
                  csv("sampleCars.csv").
                  as[Car]

    carsDS.show(5)
    carsDS.index(RTreeType, "carsRT", Array("car_lon", "car_lat"))
    carsDS.printSchema
    
    
    schema = ScalaReflection.schemaFor[Traj].dataType.asInstanceOf[StructType]
    val trajDS = simba.read.
                  option("header", "true").
                  schema(schema).
                  csv("trajectories.csv").
                  as[Traj]
     trajDS.show(5)
     trajDS.index(RTreeType, "trajsRT", Array("traj_lon", "traj_lat"))
     trajDS.printSchema()
     
    
     
   /*
       schema = ScalaReflection.schemaFor[POI_old].dataType.asInstanceOf[StructTy
     val pois_oldDS = simba.read.
                  option("header", "true").
                  schema(schema).
                  csv("POIs.csv").
                  as[POI]
    
     pois_oldDS.show(5)
     pois_oldDS.index(RTreeType, "poisoldRT", Array("poi_lon", "poi_lat"))
     pois_oldDS.printSchema()
     simba.stop() 
    */
    
  }
}