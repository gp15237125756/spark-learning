
name := "HbaseOperation"

version := "0.1"

scalaVersion := "2.11.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3"
libraryDependencies += "org.apache.spark" % "spark-hive_2.11" % "2.4.3"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.4.3"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.11" % "1.6.3"
libraryDependencies += "org.apache.spark" % "spark-streaming-flume_2.11" % "2.4.3"
libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.32"
libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.1.5"
libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.1.5"
libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.1.5"
libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.4.3"


