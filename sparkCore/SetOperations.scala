package org.apache.spark.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import com.datastax.driver.core.Row
import com.datastax.spark.connector.CassandraRow
import com.datastax.spark.connector.types.UUIDType
import org.apache.spark.sql.cassandra.CassandraCatalog

/**
 * @author verma
 */
object SetOperations {
  
    def main(args: Array[String]){
      
      val conf = new SparkConf().setAppName("Spark Cassandra Connector").setMaster("local[*]").set("spark.cassandra.connection.host", "127.0.0.1")
      val sc = new SparkContext(conf)
      
      val moviesRDD = sc.cassandraTable("firstkeyspace", "movies")
                  .select("moviename")
                  .keyBy(row => row.getString("moviename"))
                  
      val hitMoviesRDD = sc.cassandraTable("firstkeyspace", "hitmovies")
                  .keyBy(row => row.getString("moviename"))
                  
     hitMoviesRDD.leftOuterJoin(moviesRDD)
                  .filter{case (key, (rowH, rowM)) => rowM.isDefined}
                  .map{case (key, (rowH, rowM)) => rowH}
                  .collect
                  .foreach(println)
                  
      println("CassandraCount: "+ sc.cassandraTable("firstkeyspace", "movies").cassandraCount())
                  
      println(moviesRDD.partitions)
      println(moviesRDD.partitions.size)
      
      println(sc.defaultParallelism) //No of cores in System
      
      moviesRDD.repartition(2*sc.defaultParallelism).cache()
      println(moviesRDD.partitions.size)
      
      println("Cassandra Grouping-spanByKey() ")
      sc.cassandraTable("firstkeyspace", "movies")
                 .select("moviename", "movietype", "grossearning")
                 .as((mn:String, mt:String, ge:Double) => ((mn, mt), ge))
                 .spanByKey
                 .collect()
                 .foreach(println)
      
      println("Cassandra Grouping-spanBy(f) ")
      sc.cassandraTable[(String,String,Double)]("firstkeyspace", "movies")
                 .select("moviename", "movietype", "grossearning")
                 .spanBy{case (mn, mt ,ge) => (mn, mt)}
                 .collect()
                 .foreach(println)
      
      println("Joining Cassandra Tables: Optiomal way")
      moviesRDD.joinWithCassandraTable("firstkeyspace", "hitmovies").collect.foreach(println)
      
      println("Joining Cassandra Tables with on(columns): Optiomal way")
      moviesRDD.joinWithCassandraTable("firstkeyspace", "hitmovies")
               .on(SomeColumns("moviename")).takeSample(false, 100, 0).foreach(println)
              
     }
}