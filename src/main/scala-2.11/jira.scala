/**
  * Created by arvindsubramaniam on 4/8/17.
*/
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions.{array, col, explode, lit, when}

import org.apache.hadoop.fs.{FileSystem,Path}

import com.typesafe.config._

object jira {
  def main (args:Array[String]): Unit = {
    val inputPath = args(0)
    val outputPathFull = args(1)
    val outputPathIssueLink = args(2)
    val outputPathBugs = args(3)
    val outputPathlabels = args(4)
    val outputPathIssueLinkInward = args(6)

    val appConf = ConfigFactory.load()

    val conf = new SparkConf().
      setAppName(appConf.getString("appName")).
      setMaster(appConf.getConfig(args(5)).getString("deploymentMaster"))
    val sc = new SparkContext(conf)

    sc.setLogLevel(appConf.getString("appLogLevel"))
    //numShufflePartitions
    val sqlContext = SparkSession.builder().
      appName(appConf.getString("appName")).
      config("spark.sql.shuffle.partitions",appConf.getString("numShufflePartitions")).
      master(appConf.getConfig(args(5)).getString("deploymentMaster")).
      getOrCreate()

    val jsonRDD = sc.wholeTextFiles(inputPath).map(x=>x._2)
    val json = sqlContext.read.json(jsonRDD)


    val names = Seq("key","assignee","created","creator","sprint","Epic","EpicName","issueType","labels","priority","status","summary","updated","outwardIssue", "inwardIssue")

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val inputFileExists = fs.exists(new Path(inputPath))

    if (!inputFileExists) {
      println ("Input file not found")
      return
    }

    fs.delete(new Path(outputPathFull),true)
    fs.delete(new Path(outputPathIssueLink),true)
    fs.delete(new Path(outputPathBugs),true)
    fs.delete(new Path(outputPathlabels),true)

    val flatten = json.select(
                "key","fields.assignee.displayName",
                "fields.created","fields.creator.displayName",
                "fields.customfield_10007",
                "fields.customfield_10008",
                "fields.customfield_10009","fields.issuetype.name","fields.labels",
                "fields.priority.name","fields.status.name",
                "fields.summary","fields.updated","fields.issuelinks.outwardIssue",
                "fields.issuelinks.inwardIssue").
                toDF(names: _*)


    val s = flatten.withColumn("sprint", explode(
      when(col("sprint").isNotNull, col("sprint"))
        // If null explode an array<string> with a single null
        .otherwise(array(lit(null).cast("string")))))

    val l = s.withColumn("labels", explode(
      when(col("labels").isNotNull, col("labels"))
        // If null explode an array<string> with a single null
        .otherwise(array(lit(null).cast("string")))))

    val f = s.withColumn("outwardIssue",explode(col("outwardIssue")))

    val i = s.withColumn("inwardIssue",explode(col("inwardIssue")))

    s.createOrReplaceTempView("allData")
    f.createOrReplaceTempView("outwardIssue")
    l.createOrReplaceTempView("labels")
    i.createOrReplaceTempView("inwardIssue")

    val allData  = sqlContext.sql("select key,assignee,created,creator,split(sprint, ',')[3] as sprint," +
      "Epic,EpicName,issueType,priority,status,summary,updated from allData")

    val outwardIssue = sqlContext.sql("select key,assignee,created,creator,split(sprint, ',')[3] as sprint," +
      "Epic,EpicName,issueType,priority,status,summary,updated," +
      "a.outwardIssue.key as okey,a.outwardIssue.fields.issuetype.name as oissuetype," +
      "a.outwardIssue.fields.priority.name as opriority," +
      "a.outwardIssue.fields.status.name as ostatus from outwardIssue a " +
      "where status not in ('Closed','Resolved') and a.outwardIssue.key not like '%ARCH%' " +
      " AND a.outwardIssue.fields.status.name not in ('Closed','Resolved','Done','UAT Deployed') ")

    val inwardIssue = sqlContext.sql("select key,assignee,created,creator,split(sprint, ',')[3] as sprint," +
      "Epic,EpicName,issueType,priority,status,summary,updated," +
      "a.inwardIssue.key as ikey,a.inwardIssue.fields.issuetype.name as iissuetype," +
      "a.inwardIssue.fields.priority.name as ipriority," +
      "a.inwardIssue.fields.status.name as istatus from inwardIssue a " +
      "where status not in ('Closed','Resolved')  " +
      " AND a.inwardIssue.fields.status.name not in ('Closed','Resolved','Done','UAT Deployed') ")

    val labels = sqlContext.sql("select distinct key,assignee,created,creator, " +
        "split(sprint, ',')[3] as sprint," +
        "Epic,EpicName,issueType,labels,priority,status,summary,updated from labels where " +
         "status not in ('Closed','Resolved') " )

    allData.write.format("com.databricks.spark.csv").option("header", "true").save(outputPathFull)
    outwardIssue.write.format("com.databricks.spark.csv").option("header", "true").save(outputPathIssueLink)
    inwardIssue.write.format("com.databricks.spark.csv").option("header", "true").save(outputPathIssueLinkInward)
    labels.write.format("com.databricks.spark.csv").option("header", "true").save(outputPathlabels)

    val bugs = sqlContext.sql("select distinct key,assignee,created,creator," +
      "Epic,issueType,priority,status,summary,updated,split(sprint, ',')[3] as sprint from allData " +
      "where (issueType = 'Bug' OR issueType = 'Technical Debt')   ")

    //bugs.write.format("com.databricks.spark.csv").option("header", "true").save(outputPathBugs)

    /*labels.rdd.map(rec => {

    }).saveAsTextFile(outputPathlabels)*/

    bugs.rdd.map(rec => {
      rec.getString(0) + "," + rec.getString(1) + "," +
      rec.getString(2) + "," + rec.getString(3) + "," +
      rec.getString(4) + "," + rec.getString(5) + "," +
      rec.getString(6) + "," + rec.getString(7) + "," +
      rec.getString(8) + "," + rec.getString(9) + "," + rec.getString(10)
    }).saveAsTextFile(outputPathBugs)
    sc.stop()
    sqlContext.stop()
  }
}
