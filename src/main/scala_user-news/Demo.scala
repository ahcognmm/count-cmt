import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo {
    def main(args: Array[String]): Unit = {

        //        val conf = new SparkConf().setAppName("UserNewsJob").setMaster("local[*]").set("spark.executor.memory", "4g").set("spark.excutor.cores", "3")
        //        conf.registerKryoClasses(Array(classOf[RDD[String]]))
        //                val sc = new SparkContext(conf)
        val sc = new SparkContext()
        try {
            val quanOfFile = args(0).toInt
            val url = args(1)
            //            val quanOfFile = 400
            //            val url = "kenh14.vn/dan-sao-hoa-du-ky-con-re-quoc-dan-tam-tang-vo-danh-ca-thap-ki-va-sao-hang-a-dinh-scandal-chan-dong-20180115175221619.chn"
            //            val job = new DeBug(quanOfFile, sc)
            //            job.getGraph(url)
            val job = new UserNewsJob(quanOfFile, sc)
            job.getUsers(url).foreach(println(_))
        } catch {
            case e: Exception => {
                e.printStackTrace()
            }
        }
    }

}
