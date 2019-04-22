/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.HashPartitioner
// import org.apache.spark.sql
// import org.apache.spark.sql._


object SimpleApp {
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("Simple Application")
		val sc = new SparkContext(conf)
		val input = sc.textFile("input.txt")
		val links = sc.parallelize(List(("MapR",List("Baidu","Blogger")),("Baidu", List("MapR")),("Blogger",List("Google","Baidu")),("Google", List("MapR")))).partitionBy(new HashPartitioner(4)).persist()
		var ranks = links.mapValues(v => 1.0)
		val contributions = links.join(ranks).flatMap { case (url, (links, rank)) => links.map(dest => (dest, rank / links.size)) }
		val collect = contributions.collect
		val ranks2 = contributions.reduceByKey((x, y) => x + y).mapValues(v => 0.15 + 0.85*v)
		val rdd = ranks.collect
		rdd.saveAsTextFile("output")
	}
}