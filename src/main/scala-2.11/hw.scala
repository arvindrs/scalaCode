/**
  * Created by arvindsubramaniam on 4/2/17.
  */
import org.apache.spark.{SparkContext, SparkConf }
//import org.apache.log4j.{Level, Logger}

object hw {
  def main (args: Array[String]) {
    println("Hello World")
    val conf = new SparkConf().setAppName("Totl Revenue").setMaster("local")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    //val rootLogger = Logger.getRootLogger().setLevel(Level.ERROR)
    println(sc.version)
    val orders = sc.textFile("/Users/arvindsubramaniam/Documents/workspace/data/retail_db/orders")
    val orderItems = sc.textFile("/Users/arvindsubramaniam/Documents/workspace/data/retail_db/order_items")

    val ordersCompleted = orders.filter(order => {
      order.split(",")(3).equals("COMPLETE")||order.split(",")(3).equals("CANCELED")
    })
    println(ordersCompleted.count())

    val products = sc.textFile("/Users/arvindsubramaniam/Documents/workspace/data/retail_db/products")
  }
}
