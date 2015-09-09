/**
 * Created by aihe on 9/4/15.
 */
import com.twitter.scalding._
import util.TimeUtil

class WordCount(args: Args) extends Job(args) {
  TextLine(args("input"))
    .flatMap('line -> 'word) { line: String => tokenize(line) }
    .groupBy('word){ group => group.size }
    .write(TypedTsv[(String, Long)](args("output")))

  // Split a piece of text into individual words.
  def tokenize(text: String): Array[String] = {
    // Lowercase each word and remove punctuation.
    text.toLowerCase.replaceAll("[^a-zA-Z0-9\\s]", "").split("\\s+")
  }
}

object WordCountRunner extends App {

  import org.apache.hadoop.conf.Configuration
  import org.apache.hadoop.util.ToolRunner

  val timer = TimeUtil.withTime("WordCount running time") {
    ToolRunner.run(new Configuration, new Tool, (classOf[WordCount].getName :: args.toList).toArray)
  }
  println(s"Execution time: $timer msec")
}
