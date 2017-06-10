package org.apache.spark.sql.simba.examples
import org.apache.spark._
import org.apache.spark.SparkContext._

object Hello {
 def main(args: Array[String]): Unit = {
    println("Hello, world!")
    
    val inputFile = "myfile.txt";
      //val outputFile = args(1)
      val conf = new SparkConf().setMaster("local").setAppName("wordCount")
      // Create a Scala Spark Context.
      val sc = new SparkContext(conf)
      // Load our input data.
      val input =  sc.textFile(inputFile)
      // Split up into words.
      val words = input.flatMap(line => line.split(" "))
      // Transform into word and count.
      val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
      // Save the word count back out to a text file, causing evaluation.
      //counts.saveAsTextFile(outputFile)
       counts.collect().foreach(println);
       sc.stop();
       
      
      
  }
}