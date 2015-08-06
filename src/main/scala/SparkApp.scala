import org.apache.spark.{SparkContext, SparkConf}

object SparkApp extends App {
  val conf = new SparkConf().setAppName("WordCount")setMaster("local[4]")
  val context = new SparkContext(conf)

  val data = context.textFile("data.txt")

  val results = data.flatMap(lines => lines.split("\n"))
       .flatMap(line => line.split(""))
       .flatMap(word => word.toUpperCase.toCharArray)
       .filter( x => x >= 65 && x <= 90)
       .map(character => (character,1))
       .reduceByKey(_ + _)
       .sortByKey(ascending = true)

  results.foreach(println)

  Thread.sleep(10000 * 100)
}

/*
A,7)
(B,3)
(C,1)
(E,1)
(H,2)
(I,2)
(J,1)
(K,1)
(L,1)
(M,3)
(N,3)
(O,3)
(R,2)
(T,2)
*/
