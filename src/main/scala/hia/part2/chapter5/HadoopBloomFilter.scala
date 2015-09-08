package hia.part2.chapter5

import com.twitter.algebird._
import com.twitter.scalding._
import util.TimeUtil

/**
 * Created by aihe on 9/7/15.
 */
class HadoopBloomFilter(args: Args) extends Job(args) {

  val bfMonoid = BloomFilter(10000, 0.0001)
  TextLine(args("input")).read.
    groupAll {
    _.foldLeft('line -> 'bloom)(bfMonoid.zero)((bf: BF, line: String) => bfMonoid.plus(bf, bfMonoid.create(line)))
  }
    .write(TextLine(args("output")))

}

object HadoopBloomFilterRunner extends App {

  import org.apache.hadoop.conf.Configuration
  import org.apache.hadoop.util.ToolRunner

  val timer = TimeUtil.withTime("HadoopBloomFilter running time") {
    ToolRunner.run(new Configuration, new Tool, (classOf[HadoopBloomFilter].getName :: "--local" :: args.toList).toArray)
  }
  println(s"Execution time: $timer msec")
}