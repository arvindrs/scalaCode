/**
  * Created by arvindsubramaniam on 5/3/17.
  * A closure is a function, whose return value depends on the value
  * of one or more variables declared outside this function.
  */
object Closure {
  def main (args:Array[String]): Unit = {
    println( "multiplier(1) value = " +  multiplier(1) )
    println( "multiplier(2) value = " +  multiplier(2) )

  }
  var factor = 3
  val multiplier = (i:Int) => i * factor
}
