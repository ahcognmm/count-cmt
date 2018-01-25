import org.apache.spark.{SparkConf, SparkContext}

object Demo {
    def main(args: Array[String]): Unit = {

        //        val conf = new SparkConf().setAppName("UserNewsJob").setMaster("local[*]").set("spark.executor.memory", "4g").set("spark.excutor.cores", "3")
        //        val sc = new SparkContext(conf)
        val sc = new SparkContext()
        try {
            val quanOfFile = args(0).toInt
            val url = args(1)
            val job = new UserNewsJob(quanOfFile, sc)
            job.getUsers(url).foreach(println(_))
        } catch {
            case e: Exception => {
                e.printStackTrace()
            }
        }
    }

}
