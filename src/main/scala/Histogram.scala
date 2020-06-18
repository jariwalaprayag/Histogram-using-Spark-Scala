import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Histogram {

  def main ( args: Array[ String ] ) {
    /* ... */
    val conf = new SparkConf().setAppName("Histogram")
    val sc = new SparkContext(conf)
    val c = sc.textFile(args(0)).map( line => { val a = line.split(",")
                                                (a(0).toInt,a(1).toInt,a(2).toInt) } )
    val red = c.map(r => ((1,r._1),1))
    val blue = c.map(b => ((2,b._2),1))
    val green = c.map(g => ((3,g._3),1))

    val rd = red.reduceByKey(_+_)
    val bl = blue.reduceByKey(_+_)
    val gr = green.reduceByKey(_+_)

    val output = rd.union(bl).union(gr)

    val res = output.map(op => {op._1._1 + "\t" + op._1._2 + "\t" + op._2})
    res.collect().foreach(println)
    sc.stop()
  }
}
