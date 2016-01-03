package org.apache.spark.streaming


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._

/**
* @author verma
 */
object ReadFromCD {
  
  def main(args: Array[String]){
    
    val conf = new SparkConf()
                   .setAppName("Spark Cassandra Connector")
                   .setMaster("local[*]")
                   .set("spark.cassandra.connection.host", "127.0.0.1")
                   .set("spark.cleaner.ttl", "3600")
                   
    println("Way 1 of Initializing spark streaming context")
  /*  val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Durations.seconds(60))*/

    println("Way 2 of Initializing spark streaming context")
    val ssc = new StreamingContext(conf, Durations.seconds(10))
    val sc = ssc.sparkContext
        
    val stream = ssc.socketTextStream("localhost", 9999)
    stream.flatMap(x => x.split(" ")).map(word => (word, 1)).reduceByKey(_+_).print()
    
    ssc.start()
    
    ssc.awaitTermination()
  }
  
}