package hia.part2.chapter5

import com.twitter.scalding._
import util.TimeUtil

/**
 * Created by aihe on 9/7/15.
 */
class JoinCustOrder(args: Args) extends Job(args) {

  val input1 = Csv(args("input1"), fields = ('idCust, 'name, 'phone)).read

  val input2 = Csv(args("input2"), fields = ('idCustOrder, 'idO, 'price, 'date)).read

  input1.joinWithLarger(('idCust, 'idCustOrder), input2).discard('idCustOrder)
    .write(Csv(args("output")))

}

object JoinCustOrderRunner extends App {

  import org.apache.hadoop.conf.Configuration
  import org.apache.hadoop.util.ToolRunner

  val timer = TimeUtil.withTime("JoinCustOrder running time") {
    ToolRunner.run(new Configuration, new Tool, (classOf[JoinCustOrder].getName :: "--local" :: args.toList).toArray)
  }
  println(s"Execution time: $timer msec")
}
