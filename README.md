# transitiveClosure_Spark
A program that computes the transitive closure of a graph in Spark. 

Put the .txt file in hadoop system and change the directory in the program accordingly. .txt file has the format (i,j)

Command to put a file in HDFS - "hadoop fs -put '/home/hadoop/Downloads/test.txt' /wordcount/input"


*****How to run the program*****

1. put the .scala file to project_folder/src/main/scala/SparkTC.scala
2. put the .sbt file to project_folder/
3. Build the program using command- sbt package
4. Run the program using - path_to_spark_submit --class SparkTC ~/project_folder/SparkTC/target/scala-2.11/spark-transitive-closure_2.11-1.0.jar
