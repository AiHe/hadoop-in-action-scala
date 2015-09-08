package hia.part2.chapter4

import com.twitter.scalding._
import util.TimeUtil

/**
 * Created by aihe on 9/6/15.
 */
class InnerProduct(args: Args) extends Job(args) {
  val input1 = Csv(args("input1"), fields = ('key1, 'value)).read
    .map('value -> 'val1) { value: String => value.toDouble }
    .discard('value)

  val input2 = Csv(args("input2"), fields = ('key2, 'value)).read
    .map('value -> 'val2) { value: String => value.toDouble }
    .discard('value)


  input1.joinWithSmaller(('key1, 'key2), input2)
    .discard('key1)
    .map(('val1, 'val2) ->('key3, 'prod)) { value: (Double, Double) => (1, value._1 * value._2) }
    .project(('key3, 'prod))
    .groupBy('key3) {
    _.sum[Double]('prod)
  }.discard('key3)
    .write(TextLine(args("output")))
}

object InnerProductRunner extends App {

  import org.apache.hadoop.conf.Configuration
  import org.apache.hadoop.util.ToolRunner

  val timer = TimeUtil.withTime("InnerProduct running time") {
    ToolRunner.run(new Configuration, new Tool, (classOf[InnerProduct].getName :: "--local" :: args.toList).toArray)
  }
  println(s"Execution time: $timer msec")
}
