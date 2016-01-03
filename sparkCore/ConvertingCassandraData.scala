package org.apache.spark.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.datastax.spark.connector._

/**
 * @author verma
 */
object ConvertingCassandraData {
  
  case class CassandraRecord(moviename: String, movietype: String, grossearning: Double)
    
  def main(args: Array[String]){
    
    val conf = new SparkConf().setAppName("Converting Cassandra Data").setMaster("local[*]").set("spark.cassandra.connection.host", "localhost")
    val sc = new SparkContext(conf)
    
    println("\n\nCassandra Rows to tuple using CassandraTable")
    sc.cassandraTable[(String, String, Double)]("firstkeyspace", "movies")
                   .select("moviename", "movietype", "grossearning")
                   .limit(10)
                   .collect()
                   .foreach(println)
                   
    println("\n\nCassandra Rows to tuple using as(f)")
    sc.cassandraTable("firstkeyspace", "movies")
                   .select("moviename", "movietype", "grossearning").as((mn:String, mt:String, ge:Double) => (mn, mt, ge))
                   .limit(10)
                   .collect()
                   .foreach(println)
    
    println("\n\nCassandra Rows to Object using CassandraTable")  
    sc.cassandraTable[CassandraRecord]("firstkeyspace", "movies")
                   .select("moviename", "movietype", "grossearning")
                   .limit(10)
                   .collect()
                   .foreach { println }
    
    println("\n\nCassandra Rows to Object using as(f)")
    sc.cassandraTable("firstkeyspace", "movies")
                   .select("moviename", "movietype", "grossearning")
                   .as((mn:String, mt:String, ge:Double) => new CassandraRecord(mn, mt, ge))
                   .limit(10)
                   .collect()
                   .foreach(println)
                   
            
  }
  
}