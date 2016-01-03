# SparkCassandraConnector
Practice Spark Cassandra Connector in Scala


1.create a keyspace and table into cassandra:
  keyspacename: firstkeyspace
  table name : movies
  table columns: MovieId,MovieName,MovieType,GrossEarning

2.Copy the movies the csv file into the directory from where cqlsh is triggered

3.Dump movies.csv to cassandra using copy command:
  copy movies(MovieId,MovieName,MovieType,GrossEarning) from "movies.csv" with HEADER=True;
