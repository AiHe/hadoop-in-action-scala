/**
 * Created by aihe on 9/4/15.
 */
import com.twitter.scalding._

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
