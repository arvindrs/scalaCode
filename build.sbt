name := "retail_db"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.0.1"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.0.1"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.0.1"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-8_2.11" % "2.0.1"

libraryDependencies += "org.apache.bahir" %% "spark-streaming-twitter" % "2.0.1"


libraryDependencies += "com.typesafe" % "config" % "1.3.1"
libraryDependencies += "org.apache.commons" % "commons-csv" % "1.1"
libraryDependencies += "com.databricks" % "spark-csv_2.11" % "1.2.0"

//libraryDependencies ++= Seq(
 // "org.apache.spark" % "spark-core_2.11" % "1.6.0" exclude ("org.apache.hadoop","hadoop-yarn-server-web-proxy"),
  //"org.apache.spark" % "spark-sql_2.11" % "1.6.0" exclude ("org.apache.hadoop","hadoop-yarn-server-web-proxy"),
  //"org.apache.hadoop" % "hadoop-common" % "2.7.0" exclude ("org.apache.hadoop","hadoop-yarn-server-web-proxy"),
  //"org.apache.spark" % "spark-sql_2.11" % "1.6.0" exclude ("org.apache.hadoop","hadoop-yarn-server-web-proxy"),
  //"org.apache.spark" % "spark-hive_2.11" % "1.6.0" exclude ("org.apache.hadoop","hadoop-yarn-server-web-proxy"),
  //"org.apache.spark" % "spark-yarn_2.11" % "1.6.0" exclude ("org.apache.hadoop","hadoop-yarn-server-web-proxy")
//)