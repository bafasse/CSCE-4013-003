/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.HashPartitioner
// import org.apache.spark.sql
// import org.apache.spark.sql._


object SimpleApp {
	def main(args: Array[String]) {
		val iters = if (args.length > 1) args(1).toInt else 10
		val conf = new SparkConf().setAppName("Simple Application")
		val sc = new SparkContext(conf)
		val input = sc.textFile("input.txt")
		val links = input.map{ s => 
			val parts = s.split("\\s+")
      		(parts(0), parts(1))
    		}.distinct().groupByKey().cache()
    	var ranks = links.mapValues(v => 1.0)

		for (i <- 1 to iters) {
      		val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
        	val size = urls.size
        	urls.map(url => (url, rank / size))}
      		ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    	}

		// val output = ranks.collect()
    	// output.foreach(tup => println(s"${tup._1} has rank:  ${tup._2} ."))
		ranks.saveAsTextFile("output")


		// val links = sc.parallelize(List(collect)).partitionBy(new HashPartitioner(4)).persist()
		// var ranks = links.mapValues(v => 1.0)
		// val contributions = links.join(ranks).flatMap { case (url, (links, rank)) => links.map(dest => (dest, rank / links.size)) }
		// val collect = contributions.collect
		// val ranks2 = contributions.reduceByKey((x, y) => x + y).mapValues(v => 0.15 + 0.85*v)
		// val rdd = ranks.collect
		// rdd.saveAsTextFile("output")
	}
}