
import org.apache.spark.{SparkContext, SparkConf }
import org.apache.log4j.{Level, Logger}

object TopNProductsByCategory {

  def getTopNPricedProducts(rec:(Int,Iterable[String]),topN: Int): Iterable[String] = {
    //rec._2.toList.sortBy(k => -k.split(",")(4).toFloat).slice(0,topN)
    //extract prices into a collection


    val productsList = rec._2.toList
    val topNPrices = productsList.
        map(x => x.split(',')(4).toFloat).
        sortBy(x => -x).
        distinct.
        slice(0,topN)

    val topNPricedProducts = productsList.
      sortBy(x => -x.split(",")(4).toFloat).
      filter(x => topNPrices.contains(x.split(",")(4).toFloat ))
      topNPricedProducts
  }

  def main (args: Array[String]) {
      val topN = args(0).toInt
      val conf = new SparkConf().
        setAppName("Top " + topN + "Priced Products in Category - simulating dense rank").
        setMaster("local")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    Logger.getRootLogger.setLevel(Level.ERROR)
    val orders = sc.textFile("/Users/arvindsubramaniam/Documents/workspace/data/retail_db/orders")

    //println(ordersCompleted.count())

    val products = sc.textFile("/Users/arvindsubramaniam/Documents/workspace/data/retail_db/products")
    val productsFiltered = products.filter(product=>{
      product.split(",")(0).toInt!=685
  })
    val productsMap = productsFiltered.map(rec => (rec.split(",")(1).toInt,rec))
  //  productsMap.take(10).foreach(println)
    val productsGBK = productsMap.groupByKey()
   // productsGBK.take(50).foreach(println)
   //val x= productsGBK.flatMap(rec=>rec._2)
    //x.filter(rec => rec.split(",")(5).equals("http://images.acmesports.sports/Elevation+Training+Mask+2.0")).
    //collect.foreach(println)
    //x.collect().foreach(println)
    //println(x)
    productsGBK.flatMap(rec=>getTopNPricedProducts(rec,topN)).collect().foreach(println)

    sc.stop()
  }
}