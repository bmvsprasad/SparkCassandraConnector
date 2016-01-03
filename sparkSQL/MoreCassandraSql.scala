package org.apache.spark.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

//import org.apache.spark.sql.{SQLConf, SQLContext}

import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLContext._

import org.apache.spark.sql.cassandra.CassandraSQLContext

/**
 * @author verma
 */
object MoreCassandraCQL {
  
  def main(args: Array[String]){
    
    val conf = new SparkConf(true).setAppName("Cassandra SQL").setMaster("local[4]").set("spark.cassandra.connection.host", "localhost")
    val sc = new SparkContext(conf)
    val csc = new CassandraSQLContext(sc)
    
    val movies = csc.read.format("org.apache.spark.sql.cassandra").options(Map("keyspace" ->"firstkeyspace", "table"-> "movies")).load
    movies.show()
    
    import csc.implicits._
    import org.apache.spark.sql.functions._
    
    val movRDD =movies//.filter("movietype = 'Biggest Gross'")
          .groupBy("movietype")
          .agg(Map("*" -> "count", "grossearning" -> "sum", "grossearning" -> "avg"))
          .withColumnRenamed("COUNT(1)", "noofmovies")
          .withColumnRenamed("SUM(grossearning)", "cost")
          .withColumnRenamed("AVG(grossearning)", "average_gross_earning")
          .select("movietype", "noofmovies", "average_gross_earning")
          .orderBy(desc("noofmovies"), desc("average_gross_earning")).cache
          
    
   movRDD.show
   movRDD.explain()
   
   movRDD.write.format("org.apache.spark.sql.cassandra").options(Map("keyspace" ->"firstkeyspace", "table" -> "moviestats")).save()
    
  }
}