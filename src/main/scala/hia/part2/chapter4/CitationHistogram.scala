package hia.part2.chapter4

import com.twitter.scalding._

/**
 * Created by aihe on 9/6/15.
 */
class CitationHistogram(args: Args) extends Job(args) {
  Csv(args("input"), fields = ('citing, 'cited), skipHeader = true).read.
    map(('citing, 'cited) ->('c1, 'c2)) { line: (String, String) => (line._2.toInt, line._1.toInt) }.
    project(('c1, 'c2)).
    groupBy('c1) { group => group.size }.
    discard('c1).
    map('size ->('val, 'count)) { count: String => (count.toInt, 1) }.
    discard('size).
    groupBy('val) { group => group.size }.
    write(TypedTsv[(Int, Int)](args("output")))
}
