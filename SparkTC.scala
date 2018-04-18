//Author: Sikder Tahsin Al-Amin

import scala.util.Random                                      
import scala.collection.mutable  
import scala.io.StdIn.{readLine, readInt}                             
                                                                          
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx.{GraphLoader, PartitionStrategy}   
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import scala.collection.mutable.ArrayBuffer;         
                                                              
import org.apache.log4j.Logger                                
import org.apache.log4j.Level                                 
                                                        
object SparkTC {     
  def main(args: Array[String]) {                             
    Logger.getLogger("org").setLevel(Level.OFF)               
    Logger.getLogger("akka").setLevel(Level.OFF)    
    print("Enter depth= ")
    val depth = readInt()   

    val sparkConf = new SparkConf().setAppName("SparkTC")     
    val spark = new SparkContext(sparkConf) 

    /////process the input file - first put the file in hadoop system
    val fs = FileSystem.get(spark.hadoopConfiguration)
    val stream = fs.open(new Path("/graph/minigraph.txt"))
    var line = stream.readLine()
    var data: mutable.Set[(Int, Int)] = mutable.Set.empty   

    while (line != null) {
      val Array(x,y) = line.split(" ").map(_.toInt)
      data.+=((x, y)) 
      line = stream.readLine()
    }
    stream.close() 
                                                              
    val t1 = System.nanoTime           ////starting time                       
                                                          
    var tc = spark.parallelize(data.toSeq).cache() 

    // Because join() joins on keys, the edges are stored in reversed order.
    val edges = tc.map(x => (x._2, x._1))
    val tc_copy=tc

    // This join is iterated until a fixed point is reached.
    var tc_temp = tc
    //do {
    for( i <- 2 to depth)
    {
      // Perform the join, obtaining an RDD of (y, (z, x)) pairs,
      // then project the result to obtain the new (x, z) paths.
      if(i==2)
      {
        //tc_temp=tc_temp.map(x => (x._2, x._1))
        tc_temp = tc_temp.join(edges).map(x => (x._2._2, x._2._1)).distinct().cache()  //get table R2
        //println("2 executed")
      }
      else if (i==3)
      {
        tc_temp=tc_temp.map(x => (x._2, x._1))
        tc_temp = tc_temp.filter(x=>x._1 != x._2) //removing the circles 
        tc_temp = tc_temp.join(tc_copy).map(x => (x._2._2, x._2._1)).distinct().cache() ///get Table R3
        //println("3 executed")
      }
      else 
      {
        tc_temp = tc_temp.filter(x=>x._1 != x._2) //removing the circles 
        println("After eliminating circle, "+i+" edges = " + tc_temp.count())
        tc_temp = tc_temp.join(tc_copy).map(x => (x._2._2, x._2._1)).distinct().cache() ///get tables like R4,R5...
      }

      println("Tc_temp "+i+" edges = " + tc_temp.count())
      tc=tc.union(tc_temp) // R = R1 U R2 U...
      println(tc.count())
    
    }
    
    val duration = (System.nanoTime - t1) / 1e9d

    println("The program finishes in .....")
    println(duration)

    println("TC has " + tc.count() + " edges.")
    spark.stop()
  }
}

