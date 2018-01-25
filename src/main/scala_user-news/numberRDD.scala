import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object numberRDD {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("UserNewsJob").setMaster("local[*]")
        val sc = new SparkContext(conf)

        var testRDD: RDD[String] = sc.parallelize(Array("hello", "from", "the", "other", "side"))
        val testRS = testRDD.zipWithUniqueId()

        testRS.foreach(println(_))
    }

}
