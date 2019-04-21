/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SimpleApp {
	def main(args: Array[String]) {
		if (args.length < 1) {
			System.err.println("Usage: SparkPageRank <file> <iter>")
			System.exit(1)
		}

		// showWarning()

		// val spark = SparkSession
		// .builder
		val conf = new SparkConf().setAppName("Simple Application")
		// .getOrCreate()

		val iters = if (args.length > 1) args(1).toInt else 10
		val sc = new SparkContext(conf)
		val lines = sc.textFile("input.txt")
		val links = lines.map{ s =>
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

		val rdd = ranks.collect()
		rdd.foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))
		// rdd.saveAsTextFile("output")

		// spark.stop()
	}
}