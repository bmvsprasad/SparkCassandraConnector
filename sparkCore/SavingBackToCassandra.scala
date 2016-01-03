package org.apache.spark.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.datastax.spark.connector._

/**
 * @author verma
 */
object SavingBackToCassandra {
  
  case class CassandraRecord(moviename: String, movietype: String, grossearning: Double)
    
  def main(args: Array[String]){
    
    val conf = new SparkConf().setAppName("Converting Cassandra Data").setMaster("local[*]").set("spark.cassandra.connection.host", "localhost")
    val sc = new SparkContext(conf)
    
    println("Writing Cassandra Row to cassandra Table")
    val moviesRDD = sc.cassandraTable("firstkeyspace", "movies")
                      .select("moviename", "movietype", "grossearning")
                      .filter(row => (row.getString("movietype").toLowerCase().equals("biggest gross") || row.getString("movietype").toLowerCase().equals("best bicture")) && row.getDouble("grossearning")>200)
    //moviesRDD.saveToCassandra("firstkeyspace", "hitmovies") 
    
    println("Writing Objects to cassandra Table") 
    /*moviesRDD.map(obj => new CassandraRecord(obj.getString("moviename"), obj.getString("movietype"), obj.getDouble("grossearning")))
                      .saveToCassandra("firstkeyspace", "hitmovies")*/
                      
    println("Writing Tuples to cassandra Table")    
    moviesRDD.map(row => (row.getString("moviename"), row.getString("movietype"),row.getDouble("grossearning")))
             .saveToCassandra("firstkeyspace", "hitmovies")
    
  }
}