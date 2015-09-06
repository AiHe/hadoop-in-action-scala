package inaction.part2.chapter4

import com.twitter.scalding._

/**
 * Created by aihe on 9/5/15.
 */
class GroupCitation(args: Args) extends Job(args) {
  Csv(args("input"), fields = ('citing, 'cited)).read.
    map(('citing, 'cited) ->('c1, 'c2)) { line: (String, String) => (line._2, line._1) }.
    project(('c1, 'c2)).
    groupBy('c1) { group => group }.
    write(Tsv(args("output")))
}
