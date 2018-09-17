name := "CitiSpike"

version := "0.1"

scalaVersion := "2.11.8"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.2.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.2.1"

// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-mapreduce-client-core
libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "2.6.0"

// https://mvnrepository.com/artifact/org.apache.hbase/hbase-common
libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.2.6.1"


libraryDependencies +=  "com.google.guava" % "guava" % "15.0"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.6.0"

// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-common
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.6.0"


// https://mvnrepository.com/artifact/org.apache.hbase/hbase-client
libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.2.6.1"

// https://mvnrepository.com/artifact/org.apache.hbase/hbase-server
libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.2.6.1"

libraryDependencies+= "org.apache.hbase" % "hbase" % "1.2.6.1"

libraryDependencies += "org.apache.hbase" % "hbase-hadoop-compat" % "1.2.6.1"
libraryDependencies += "org.apache.hbase" % "hbase-it" % "1.2.6.1"
libraryDependencies += "org.apache.hbase" % "hbase-hadoop2-compat" % "1.2.6.1"

libraryDependencies += "org.apache.hbase" % "hbase-prefix-tree" % "1.2.6.1"
libraryDependencies += "org.apache.hbase" % "hbase-protocol" % "1.2.6.1"

libraryDependencies += "org.apache.hbase" % "hbase-shell" % "1.2.6.1"

libraryDependencies += "org.apache.hbase" % "hbase-testing-util" % "1.2.6.1"
libraryDependencies += "org.apache.hbase" % "hbase-thrift" % "1.2.6.1"