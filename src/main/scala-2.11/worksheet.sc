import org.apache.spark.{SparkContext, SparkConf }
import org.apache.spark.rpc.netty
import org.apache.spark._

val conf = new SparkConf().setAppName("Totl Revenue").setMaster("local")

val sc = new SparkContext(conf)

sc.stop()