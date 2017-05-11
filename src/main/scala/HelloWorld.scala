import org.apache.spark.{SparkConf, SparkContext}

/**
  * Hello world (word count).
  */
object HelloWorld {

  def main(args: Array[String]): Unit = {

    val env = System.getenv()

    // SparkConf
    val conf = new SparkConf()
      .setAppName("hello-world")
      .setMaster(env.getOrDefault("MASTER", "local[2]"))

    // Get spark session
    val spark = new SparkContext(conf)
    val phrases = spark.parallelize(Seq("Hello world"))

    val counts = phrases.flatMap(l => l.split(" ")).map(w => (w, 1)).reduceByKey(_ + _)

    // Print result
    counts.collect.foreach {
      case (w: String, c: Int) => println(s"$w -> $c times")
    }

    spark.stop()
  }

}
