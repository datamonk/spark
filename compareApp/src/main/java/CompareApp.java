/* CompareApp.java  */
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;

object test {
def main(args: Array[String]): Unit = {
val conf = new SparkConf().setAppName("prep").setMaster("local")
val sc = new SparkContext(conf)
val searchList = sc.textFile("data/words.txt")

val sentilex = sc.broadcast(searchList.map({ (line) =>
  val Array(a,b) = line.split(",").map(_.trim)
  (a,b.toInt)
  }).collect().toMap)    

val sample1 = sc.textFile("data/data.txt")
val sample2 = sample1.map(line=>(line.split(" ").map(word => sentilex.value.getOrElse(word, 0)).reduce(_ + _), line))
sample2.collect.foreach(println)
 }
}
