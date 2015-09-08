package util

/**
 * Created by aihe on 9/7/15.
 */
object TimeUtil {

  def withTime(description: String)(block: => Unit): Long = {
    println(description)
    val start = System.currentTimeMillis()
    var end = 0l
    try {
      block
    } finally {
      end = System.currentTimeMillis()
    }
    end - start
  }

}
