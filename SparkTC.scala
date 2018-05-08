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
/**                                                           
 * Transitive closure on a graph.                             
 */                                                           
object SparkTC {                                                                                                                                                                                                      
                                                            
  def main(args: Array[String]) {                             
    Logger.getLogger("org").setLevel(Level.OFF)               
    Logger.getLogger("akka").setLevel(Level.OFF)    
    print("Enter depth= ")
    val depth = readInt()   

    val sparkConf = new SparkConf().setAppName("SparkTC")     
    val spark = new SparkContext(sparkConf) 

    /////process the input file
    val fs = FileSystem.get(spark.hadoopConfiguration)
    val stream = fs.open(new Path("/graph/treecliquelinear995424.txt"))
    var line = stream.readLine()
    var data: mutable.Set[(Int, Int)] = mutable.Set.empty   

    while (line != null) {
      val Array(x,y) = line.split(" ").map(_.toInt)
      data.+=((x, y)) 
      line = stream.readLine()
    }
    stream.close() 
                                                              
    val t1 = System.nanoTime           ////starting time                       
             
    //var generateGraph = GraphLoader.edgeListFile(spark, "/graph/minigraph.txt",true).partitionBy(PartitionStrategy.RandomVertexCut)               
    //val edges2=Set((3,0), (1,2), (4,1), (3,1), (0,2), (4,2), (2,0), (1,0), (2,1), (1,3))
    var tc = spark.parallelize(data.toSeq).cache() 

    // Linear transitive closure: each round grows paths by one edge,
    // by joining the graph's edges with the already-discovered paths.
    // e.g. join the path (y, z) from the TC with the edge (x, y) from
    // the graph to obtain the path (x, z).

    // Because join() joins on keys, the edges are stored in reversed order.
    val edges = tc.map(x => (x._2, x._1))
    val tc_copy=tc

    // This join is iterated until a fixed point is reached.
    var tc_temp = tc
    
    for( i <- 2 to depth)
    {
      // Perform the join, obtaining an RDD of (y, (z, x)) pairs,
      // then project the result to obtain the new (x, z) paths.
      if(i==2)
      {
        //tc_temp=tc_temp.map(x => (x._2, x._1))
        tc_temp = tc_temp.join(edges).map(x => (x._2._2, x._2._1)).distinct().cache()
        //println("2 executed")
      }
      else if (i==3)
      {
        tc_temp=tc_temp.map(x => (x._2, x._1))
        tc_temp = tc_temp.filter(x=>x._1 != x._2) //removing the circles 
        tc_temp = tc_temp.join(tc_copy).map(x => (x._2._2, x._2._1)).distinct().cache() ///intermediate tables like r2,r3...
        //println("3 executed")
      }
      else 
      {
        tc_temp = tc_temp.filter(x=>x._1 != x._2) //removing the circles 
        //println("After eliminating circle, "+i+" edges = " + tc_temp.count())
        tc_temp = tc_temp.join(tc_copy).map(x => (x._2._2, x._2._1)).distinct().cache() ///intermediate tables like r2,r3...
      }

      //println("Tc_temp "+i+" edges = " + tc_temp.count())
      tc=tc.union(tc_temp) // R1 U R2 U...
      //println(tc.collect())
      //nextCount = tc.count()
      //tc = tc.filter(x=>x._1 != x._2)
      //println(tc.count())   
    }
    
    val duration = (System.nanoTime - t1) / 1e9d

    println("The program finishes in .....")
    println(duration)

    println("TC has " + tc.count() + " edges.")
    spark.stop()
  }
}
