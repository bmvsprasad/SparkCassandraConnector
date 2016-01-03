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
object CassandraCQL {
  
  case class newMovie(name:String, mtype:String, earning:Double)
  
  def main(args: Array[String]){
    
    val conf = new SparkConf(true).setAppName("Cassandra SQL").setMaster("local[4]").set("spark.cassandra.connection.host", "localhost")
    val sc = new SparkContext(conf)
    val csc = new CassandraSQLContext(sc)
    
    val movies = csc.read.format("org.apache.spark.sql.cassandra").options(Map("keyspace" ->"firstkeyspace", "table"-> "hitmovies")).load
    
    movies.filter("movietype = 'My Fav'").show
    movies.show()
    
    import csc.implicits._
    println("\nInferring the Schema-Way1")
    val rdd1 = sc.parallelize(Array(newMovie("X-Men", "Sci-fi", 300.50), newMovie("Spaider Man", "Sci-fi", 150.90)))
    val df1 = rdd1.toDF()
    df1.show()
    
    println("\nInferring the Schema-Way2")
    val rdd2 = sc.parallelize(Array(("X-Men", "Sci-fi", 300.50), ("Spider Man", "Sci-fi", 150.90)))
    val df2 = rdd2.toDF("name", "mtype", "earning")
    df2.show()
    
    println("RDD of Rows")
    val rowRDD = sc.parallelize(Array(("X-Men", "Sci-fi", 300.50), ("Spider Man", "Sci-fi", 150.90)))
                  .map{case(mn, mt, ge) => Row(mn, mt, ge)}
    
    val schema = StructType(List(StructField("moviename", StringType, false), StructField("movietype", StringType, false), StructField("grossearning", DoubleType, false)))
    val df = csc.createDataFrame(rowRDD, schema)
    df.show()
    df.printSchema()
    
    val movRDD = csc.sql("Select * from firstkeyspace.hitmovies")
    movRDD.dtypes
    println(movRDD.schema)
    
    val df11 = movRDD.first
    
    println(df11(0))
    println(df11(1))    
    if(!(df11.isNullAt(2)))
      println(df11(2))
  }
  
}