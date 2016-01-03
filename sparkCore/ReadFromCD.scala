package org.apache.spark.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import com.datastax.driver.core.Row
import com.datastax.spark.connector.CassandraRow

/**
* @author verma
 */
object ReadFromCD {
  
  def main(args: Array[String]){
    
    val conf = new SparkConf().setAppName("Spark Cassandra Connector").setMaster("local[*]").set("spark.cassandra.connection.host", "127.0.0.1")
    val sc = new SparkContext(conf)
    
    val moviesRDD = sc.cassandraTable("firstkeyspace", "movies")
                .select("moviename", "movietype", "grossearning")
                //.where("moviename = 'Shrek' and movietype = 'Series'")
                //.withAscOrder
                //.limit(10)
                
    println(moviesRDD.columnNames)
    println(moviesRDD.count)
    println(moviesRDD.first())
    
    println("\nList of All movies")
    moviesRDD.collect.foreach(println)
  
    println("\n\nMovies with ,the in its Name")
    moviesRDD.filter(row => row.getString("moviename").toLowerCase.contains(", the"))
                            .map(row => row.getString("moviename")+": "+ row.getString("movietype")+": "+ row.getDouble("grossearning"))
                            .collect
                            .foreach(println)
    
    println("\n\nMovies with type sundance and grossearning > 20M")
    moviesRDD.filter(row => row.get[String]("movietype").toLowerCase().equals("sundance") && row.get[Double]("grossearning") > 15)
                               .map(row => row.get[String]("moviename")+" : "+ row.get[String]("movietype")+" : "+row.get[Double]("grossearning"))
                               .collect()
                               .foreach(println)
       
  }
  
}