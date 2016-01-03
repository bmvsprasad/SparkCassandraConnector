package org.apache.spark.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import com.datastax.driver.core.Row
import com.datastax.spark.connector.CassandraRow

/**
* @author verma
 */
object PairRDDs {
  
  def main(args: Array[String]){
    
    val conf = new SparkConf().setAppName("Spark Cassandra Connector").setMaster("local[*]").set("spark.cassandra.connection.host", "127.0.0.1")
    val sc = new SparkContext(conf)
    
    val moviesRDD = sc.cassandraTable[(String, Option[Double])]("firstkeyspace", "movies")
                .select("movietype", "grossearning").cache()
      
    moviesRDD.mapValues(v => v.getOrElse(0.0))
                .lookup("Sundance")
                .foreach(println)
                
    println("Transformation Using reduceByKey: How many movies of each Type")
    moviesRDD.map(row => (row._1, 1)).reduceByKey(_+_).collect.foreach(println)
    
    println("\nTransformation Using reduceByKey: Total of Gross earning of each Type")
    moviesRDD.mapValues(v => v.getOrElse(0.0)).map(row => (row._1, row._2)).reduceByKey(_+_).collect.foreach(println)
        
    println("Transformation foldByKey: Same as reduceByKey except assumes an initial value- 0 for addition, 1 for multiplication and null for string concatenation")
    println("\nGet highest grossing movie of each type using foldByKey")
    moviesRDD.as((m:String, g:Double) => (m, g)).foldByKey((0.0)){(MaxG, g) =>
                                                                  if (MaxG < g) g
                                                                  else MaxG}.collect.foreach(println)
    println("\nGet highest grossing movie of each type Using reduceByKey")                                                              
    moviesRDD.mapValues(v => v.getOrElse(0.0)).reduceByKey((x, y) => 
                                                                           if (x>y) x
                                                                           else y).collect.foreach(println)
                                                                           
    println("Average gross earning of each type of movie: Using combineByKey")
    moviesRDD.mapValues(v => v.getOrElse(0.0)).combineByKey((ge:Double) =>(ge, 1), 
                          (res:(Double,Int), ge:Double) => (res._1+ge, res._2+1), 
                          (res1: (Double, Int), res2: (Double, Int)) => (res1._1+res2._1, res1._2+res2._2))
                          .mapValues{case (sum, count) => val avg = sum/count; f"$avg%1.2f"}
                          .collect
                          .foreach(println)
     
    println("Average gross earning of each type of movie: Using reduceByKey")
    val averageGross = moviesRDD.mapValues(v => v.getOrElse(0.0))
                          .map{ case (key, value) => (key, (value, 1)) }
                          .reduceByKey{case ((value1, count1), (value2, count2)) => (value1 + value2, count1 + count2)}
                          .mapValues{case (sum, count) => val avg = sum/count; f"$avg%1.2f"}.cache()
     averageGross.collect.foreach(println)
     
    println("Count of each type of movie: Using reduceByKey")
    moviesRDD.map(row => (row._1, 1)).countByKey.foreach(println)
    
    println("Transformation using groupByKey")
    moviesRDD.groupByKey().collect.foreach(println)
    
    println(averageGross.partitioner)
    println(averageGross.partitioner.size)
     
    
    }
}