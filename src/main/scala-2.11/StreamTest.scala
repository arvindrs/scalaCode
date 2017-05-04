/**
  * Created by arvindsubramaniam on 5/1/17.
  */
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf}
import org.apache.spark.streaming._

object StreamTest {
  def main(args:Array[String]): Unit = {

    val updateFunction = (values: Seq[Int], runningCount: Option[Int])=> {
      val newCount = values.sum + runningCount.getOrElse(0)
       Some(newCount)
    }

    val conf  = new SparkConf().setMaster("local[2]").setAppName("Test Streaming")
    val ssc = new StreamingContext(conf,Seconds(10))
    val lines = ssc.socketTextStream("localhost",9999,StorageLevel.MEMORY_AND_DISK_SER_2)
    ssc.checkpoint("cp")
    val words = lines.flatMap(rec=>rec.split(" "))
    val pairs = words.map(rec => (rec,1))
    val wordCount = pairs.reduceByKey((x,y)=>x+y)

    val totalwordCount = wordCount.updateStateByKey(updateFunction)
    totalwordCount.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
