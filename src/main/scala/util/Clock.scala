package util

object Clock {

  def measure[A](operation: String)(fn: => A): A = {
    val start = System.currentTimeMillis()
    val res = fn
    val end = System.currentTimeMillis()
    println(s"Operation [$operation] finished in ${end - start} millis")
    res
  }

}
