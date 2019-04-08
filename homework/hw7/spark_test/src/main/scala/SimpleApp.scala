/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SimpleApp {
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("Simple Application")
		val sc = new SparkContext(conf)
		val input = sc.textFile("input.txt")
		val words = input.flatMap(line => line.split(" "))
		val rdd = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
		rdd.saveAsTextFile("output")
		sc.stop()
	}
}